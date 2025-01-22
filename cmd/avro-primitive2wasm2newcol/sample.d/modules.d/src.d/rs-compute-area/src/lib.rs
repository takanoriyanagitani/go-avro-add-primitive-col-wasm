pub fn _wh2area(width: f64, height: f64) -> f64 {
    width * height
}

#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn wh2area(width: u64, height: u64) -> u64 {
    let w: f64 = bits2double(width);
    let h: f64 = bits2double(height);
    let area: f64 = _wh2area(w, h);
    double2bits(area)
}

#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn double2bits(d: f64) -> u64 {
    d.to_bits()
}

#[allow(unsafe_code)]
#[no_mangle]
pub extern "C" fn bits2double(b: u64) -> f64 {
    f64::from_bits(b)
}
