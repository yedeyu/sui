module 0x42::m {

    public enum M {
        T { x: u64 }
    }

    fun t(): u64 {
        let m = M::T { x: 6 };
        m.x
    }

}
