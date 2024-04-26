use std::println;

#[no_mangle]
pub fn init() {
    println!("init: 1");
}

#[no_mangle]
pub fn deinit() {
    println!("deinit: 1");
}
