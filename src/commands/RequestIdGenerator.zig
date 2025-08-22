var request_id: i32 = 0;

pub fn getNextRequestId() i32 {
    return @atomicRmw(i32, &request_id, .Add, 1, .monotonic);
}
