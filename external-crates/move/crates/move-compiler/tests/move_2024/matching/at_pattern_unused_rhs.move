module 0x42::m {

    public enum Maybe<T> {
        Just(T),
        Nothing
    }

    fun t<T>(m: Maybe<T>) {
        match (m) {
            x @ Maybe::Just(_) if (valid(x)) => (),
            Maybe::Just(_) => (),
            Maybe::Nothing => ()
        }
    }

    fun valid<T>(_m: &Maybe<T>): bool { true }

}
