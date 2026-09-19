//! CRC-32C (Castagnoli), hardware on aarch64 with the `crc` feature and a slicing-by-8
//! table otherwise. Both are here so B13 can report the software cost next to the
//! hardware one.

const POLY: u32 = 0x82F63B78;

fn table() -> &'static [[u32; 256]; 8] {
    static T: std::sync::OnceLock<[[u32; 256]; 8]> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        let mut t = [[0u32; 256]; 8];
        for i in 0..256u32 {
            let mut c = i;
            for _ in 0..8 {
                c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            }
            t[0][i as usize] = c;
        }
        for i in 0..256 {
            for k in 1..8 {
                t[k][i] = (t[k - 1][i] >> 8) ^ t[0][(t[k - 1][i] & 0xff) as usize];
            }
        }
        t
    })
}

pub fn software(mut crc: u32, data: &[u8]) -> u32 {
    let t = table();
    crc = !crc;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        let lo = u32::from_le_bytes(c[..4].try_into().unwrap()) ^ crc;
        let hi = u32::from_le_bytes(c[4..].try_into().unwrap());
        crc = t[7][(lo & 0xff) as usize]
            ^ t[6][((lo >> 8) & 0xff) as usize]
            ^ t[5][((lo >> 16) & 0xff) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xff) as usize]
            ^ t[2][((hi >> 8) & 0xff) as usize]
            ^ t[1][((hi >> 16) & 0xff) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
        crc = t[0][((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn hw_inner(mut crc: u32, data: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd};
    crc = !crc;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        crc = __crc32cd(crc, u64::from_le_bytes(c.try_into().unwrap()));
    }
    for &b in chunks.remainder() {
        crc = __crc32cb(crc, b);
    }
    !crc
}

pub fn hardware_available() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        std::arch::is_aarch64_feature_detected!("crc")
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        false
    }
}

pub fn hardware(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    {
        if hardware_available() {
            return unsafe { hw_inner(crc, data) };
        }
    }
    software(crc, data)
}

#[test]
fn agree() {
    let d: Vec<u8> = (0..10007u32).map(|i| (i * 7 % 251) as u8).collect();
    assert_eq!(software(0, b"123456789"), 0xE3069283);
    assert_eq!(software(0, &d), hardware(0, &d));
}
