module 0x42::m {

    public enum Maybe<T> {
        Just(T),
        Nothing
    }

}

module 0x42::n {
    use 0x42::m;

    fun t<T>(m: &0x42::m::Maybe<T>) {
        let _ = match (m) {
            m::Maybe::Just(_) => 5,
            m::Maybe::Nothing => 10,
        };
    }

}
