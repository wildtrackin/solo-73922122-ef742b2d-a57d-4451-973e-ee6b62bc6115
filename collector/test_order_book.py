"""盘口同步规则单元测试（不依赖网络，直接测试 OrderBook 纯逻辑）。

运行：cd collector && python -m pytest test_order_book.py -v
"""
import pytest

from app import GapDetected, OrderBook


def ev(U, u, b=None, a=None):
    return {"U": U, "u": u, "b": b or [], "a": a or []}


def snapshot(L=100):
    return {
        "lastUpdateId": L,
        "bids": [["100.0", "1.0000"], ["99.9", "2.0000"]],
        "asks": [["100.1", "1.5000"], ["100.2", "2.5000"]],
    }


def new_book():
    book = OrderBook()
    book.load_snapshot(snapshot())
    return book


def stats():
    return {"applied": 0, "dropped": 0}


def test_snapshot_loads_L_and_levels():
    book = new_book()
    assert book.last_update_id == 100
    assert book.bids == {100.0: 1.0, 99.9: 2.0}
    assert book.asks == {100.1: 1.5, 100.2: 2.5}


def test_drop_events_with_u_not_greater_than_L():
    book = new_book()
    s = stats()
    book.apply(ev(99, 100, b=[["100.0", "9.0000"]]), s)   # u == L，丢弃
    book.apply(ev(95, 98, b=[["100.0", "9.0000"]]), s)    # u < L，丢弃（重复消息）
    assert s["dropped"] == 2 and s["applied"] == 0
    assert book.bids[100.0] == 1.0  # 未被污染
    assert book.last_update_id == 100


def test_apply_straddling_event_and_advance_L():
    book = new_book()
    s = stats()
    book.apply(ev(99, 101, b=[["100.0", "3.0000"]]), s)   # U<=L+1<=u，应用
    assert book.bids[100.0] == 3.0
    assert book.last_update_id == 101
    book.apply(ev(102, 102, a=[["100.1", "0.0000"]]), s)  # 紧接着的下一条
    assert 100.1 not in book.asks
    assert book.last_update_id == 102


def test_gap_raises_and_book_untouched():
    book = new_book()
    s = stats()
    with pytest.raises(GapDetected):
        book.apply(ev(105, 106, b=[["100.0", "9.0000"]]), s)  # U=105 > L+1=101
    assert book.bids[100.0] == 1.0
    assert book.last_update_id == 100


def test_zero_quantity_deletes_level():
    book = new_book()
    s = stats()
    book.apply(ev(101, 101, b=[["99.9", "0.0000"]]), s)
    assert 99.9 not in book.bids
    assert book.bids[100.0] == 1.0


def test_quantity_is_absolute_not_delta():
    book = new_book()
    s = stats()
    book.apply(ev(101, 101, b=[["100.0", "5.0000"]]), s)
    book.apply(ev(102, 102, b=[["100.0", "7.0000"]]), s)
    assert book.bids[100.0] == 7.0  # 最新总量，而非累加


def test_trim_to_100_levels_per_side():
    snap = {
        "lastUpdateId": 1,
        "bids": [[f"{200.0 - i * 0.1:.1f}", "1.0000"] for i in range(150)],
        "asks": [[f"{200.1 + i * 0.1:.1f}", "1.0000"] for i in range(150)],
    }
    book = OrderBook()
    book.load_snapshot(snap)
    assert len(book.bids) == 100 and len(book.asks) == 100
    assert max(book.bids) == 200.0          # 保留最优 100 档
    assert min(book.asks) == pytest.approx(200.1)


# ----------------------- 新鲜度 / 行情停更规则 -----------------------

def test_advancing_event_records_actual_receive_time():
    book = new_book()
    s = stats()
    assert book.apply(ev(99, 101, b=[["100.0", "3.0000"]]), s, recv_mono=10.0) is True
    assert book.last_advance_recv_mono == 10.0
    assert book.last_advance_update_id == 101
    # 接收至今不足 3 秒 → 新鲜；满 3 秒 → 不新鲜（严格小于阈值）
    assert book.has_fresh_advance(now_mono=12.999, max_age=3.0)
    assert not book.has_fresh_advance(now_mono=13.0, max_age=3.0)


def test_duplicate_does_not_refresh_receive_time():
    book = new_book()
    s = stats()
    assert book.apply(ev(101, 102, b=[["100.0", "3.0000"]]), s, recv_mono=10.0) is True
    # 同一条/过期消息再次到达，u <= L，必须丢弃且不能续期
    assert book.apply(ev(101, 102, b=[["100.0", "9.0000"]]), s, recv_mono=20.0) is False
    assert book.last_advance_recv_mono == 10.0
    assert book.bids[100.0] == 3.0
    assert s["dropped"] == 1


def test_cache_replayed_later_keeps_original_receive_time():
    book = new_book()
    s = stats()
    # 模拟：t=1 收到，直到 t=10 才应用。新鲜度必须按 t=1 计算，不能重新计时。
    book.apply(ev(101, 101, b=[["100.0", "3.0000"]]), s, recv_mono=1.0)
    assert book.last_advance_recv_mono == 1.0
    assert not book.has_fresh_advance(now_mono=10.0, max_age=3.0)
    assert book.data_age(10.0) == pytest.approx(9.0)


def test_snapshot_alone_is_not_fresh():
    book = new_book()
    assert book.last_advance_recv_mono is None
    assert not book.has_fresh_advance(now_mono=0.0, max_age=3.0)
    assert book.data_age(0.0) is None


def test_gap_does_not_refresh_receive_time():
    book = new_book()
    s = stats()
    book.apply(ev(99, 101, b=[["100.0", "3.0000"]]), s, recv_mono=1.0)
    with pytest.raises(GapDetected):
        book.apply(ev(105, 106, b=[["100.0", "9.0000"]]), s, recv_mono=2.0)
    assert book.last_advance_recv_mono == 1.0
    assert book.bids[100.0] == 3.0
