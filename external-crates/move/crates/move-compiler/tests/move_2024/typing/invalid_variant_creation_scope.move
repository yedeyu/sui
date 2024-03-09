module 0x42::m {

    public enum F {
        A(u64)
    }

}

module 0x42::n {

    fun t(): 0x42::m::F {
        0x42::m::F::A(10)
    }

}
