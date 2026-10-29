//! An append-only collection with stable indexes: what a connection holds
//! of its class is the index, and classes can be added while connections run.

use std::{
    iter::repeat_with,
    ops::Index,
    sync::{Mutex, OnceLock, PoisonError},
};

/// The capacity is fixed, so that slots never move. Each slot is filled once,
/// in order. Reading takes no lock: a slot is a `OnceLock`, and a reader that
/// sees one filled sees every one before it. Adding takes a lock.
pub struct Registry<T> {
    slots: Box<[OnceLock<T>]>,
    /// The index of the next slot to fill, held while filling it.
    next: Mutex<usize>,
}

impl<T> Registry<T> {
    /// Empty, with room for `capacity` values.
    pub fn with_capacity(capacity: usize) -> Self {
        Registry {
            slots: repeat_with(OnceLock::new).take(capacity).collect(),
            next: Mutex::new(0),
        }
    }

    /// Adds a value after the others and returns its index. `None` when
    /// every slot is filled.
    pub fn push(&self, value: T) -> Option<usize> {
        // Nothing the guard protects is left half done by a panic.
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        let index = *next;
        let Ok(()) = self.slots.get(index)?.set(value) else {
            unreachable!("slots are filled only here, once each");
        };
        *next += 1;
        Some(index)
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.slots.get(index)?.get()
    }

    /// The values in the order they were added.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().map_while(OnceLock::get)
    }
}

impl<T> Index<usize> for Registry<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        self.get(index).expect("the index is of an added value")
    }
}

/// Full: there is no room for more.
impl<T> FromIterator<T> for Registry<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        let slots: Box<[OnceLock<T>]> = values.into_iter().map(OnceLock::from).collect();
        let next = Mutex::new(slots.len());
        Registry { slots, next }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{
            AtomicBool,
            Ordering::{Acquire, Release},
        },
        thread,
    };

    use super::*;

    fn value(index: usize) -> String {
        format!("value {index}")
    }

    fn contents(registry: &Registry<String>) -> Vec<&str> {
        registry.iter().map(String::as_str).collect()
    }

    #[test]
    fn values_get_the_next_index_and_are_found_by_it() {
        let registry = Registry::with_capacity(3);

        assert_eq!(registry.push("a"), Some(0));
        assert_eq!(registry.push("b"), Some(1));

        assert_eq!(registry.get(0), Some(&"a"));
        assert_eq!(registry.get(1), Some(&"b"));
        assert_eq!(registry[0], "a");
        assert_eq!(registry[1], "b");
    }

    #[test]
    fn a_slot_without_a_value_is_not_found() {
        let registry = Registry::with_capacity(2);
        registry.push("a");

        assert_eq!(registry.get(1), None);
        // Beyond the capacity.
        assert_eq!(registry.get(2), None);
    }

    #[test]
    #[should_panic(expected = "the index is of an added value")]
    fn indexing_a_slot_without_a_value_panics() {
        let registry = Registry::<&str>::with_capacity(2);
        let _ = registry[0];
    }

    #[test]
    fn iteration_is_in_the_order_of_adding() {
        let registry = Registry::with_capacity(4);
        assert_eq!(registry.iter().count(), 0);

        registry.push("c");
        registry.push("a");
        registry.push("b");

        assert_eq!(registry.iter().collect::<Vec<_>>(), [&"c", &"a", &"b"]);
    }

    #[test]
    fn a_full_registry_refuses_values_and_keeps_its_own() {
        let registry = Registry::with_capacity(2);
        registry.push("a");
        registry.push("b");

        assert_eq!(registry.push("c"), None);
        assert_eq!(registry.push("d"), None);

        assert_eq!(registry.iter().collect::<Vec<_>>(), [&"a", &"b"]);
        assert_eq!(registry.get(2), None);
    }

    #[test]
    fn a_registry_without_capacity_is_full() {
        let registry = Registry::with_capacity(0);

        assert_eq!(registry.push("a"), None);
        assert_eq!(registry.iter().count(), 0);
    }

    #[test]
    fn a_registry_collected_from_values_has_them_and_is_full() {
        let registry: Registry<_> = ["a", "b"].into_iter().collect();

        assert_eq!(registry.iter().collect::<Vec<_>>(), [&"a", &"b"]);
        assert_eq!(registry[1], "b");
        assert_eq!(registry.push("c"), None);
    }

    #[test]
    fn a_registry_collected_from_nothing_is_empty_and_full() {
        let registry: Registry<&str> = [].into_iter().collect();

        assert_eq!(registry.iter().count(), 0);
        assert_eq!(registry.push("a"), None);
    }

    #[test]
    fn readers_see_whole_prefixes_while_values_are_added() {
        const CAPACITY: usize = 2_000;
        let registry = Registry::with_capacity(CAPACITY);
        let done = AtomicBool::new(false);

        thread::scope(|scope| {
            let readers: Vec<_> = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        let mut seen = 0;
                        loop {
                            // Read the flag first: what the registry holds
                            // after it is set is all of it.
                            let finished = done.load(Acquire);
                            let values = contents(&registry);
                            // Values come in order, with none missing, and
                            // never go away.
                            for (index, found) in values.iter().enumerate() {
                                assert_eq!(*found, value(index));
                            }
                            assert!(values.len() >= seen);
                            seen = values.len();
                            if finished {
                                return seen;
                            }
                            if let Some(found) = registry.get(seen) {
                                assert_eq!(found, &value(seen));
                            }
                        }
                    })
                })
                .collect();
            for index in 0..CAPACITY {
                assert_eq!(registry.push(value(index)), Some(index));
            }
            done.store(true, Release);
            for reader in readers {
                assert_eq!(reader.join().unwrap(), CAPACITY);
            }
        });
    }

    #[test]
    fn concurrent_adders_get_distinct_indexes_up_to_the_capacity() {
        const CAPACITY: usize = 500;
        const ADDERS: usize = 8;
        const EACH: usize = 100;
        let registry = Registry::with_capacity(CAPACITY);

        let results: Vec<Vec<Option<usize>>> = thread::scope(|scope| {
            (0..ADDERS)
                .map(|adder| {
                    let registry = &registry;
                    scope.spawn(move || {
                        (0..EACH)
                            .map(|n| registry.push((adder, n)))
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|adder| adder.join().unwrap())
                .collect()
        });

        let mut indexes: Vec<usize> = results.iter().flatten().flatten().copied().collect();
        indexes.sort_unstable();
        assert_eq!(indexes, (0..CAPACITY).collect::<Vec<_>>());
        assert_eq!(
            results.iter().flatten().filter(|r| r.is_none()).count(),
            300
        );
        // Each adder's values are in the order it added them.
        for adder in 0..ADDERS {
            let ns: Vec<usize> = registry
                .iter()
                .filter(|&&(a, _)| a == adder)
                .map(|&(_, n)| n)
                .collect();
            assert!(ns.is_sorted(), "{ns:?}");
        }
    }
}
