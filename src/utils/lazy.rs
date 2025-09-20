use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::{Mutex, atomic::AtomicBool, atomic::Ordering};

pub struct Lazy<V: Sync> {
    initialized: AtomicBool,
    factory: Mutex<Option<Box<dyn (FnOnce() -> V) + Send>>>,
    value: UnsafeCell<Option<V>>,
}

impl<V: Sync> Lazy<V> {
    pub fn new<F: (FnOnce() -> V) + Send + 'static>(factory: F) -> Self {
        Self {
            initialized: AtomicBool::default(),
            factory: Mutex::new(Some(Box::new(factory))),
            value: UnsafeCell::new(None),
        }
    }
}

impl<V: Sync> Deref for Lazy<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        if self.initialized.load(Ordering::Acquire) {
            return unsafe { &*self.value.get() }.as_ref().unwrap();
        }
        {
            let mut factory = self.factory.lock().unwrap();
            if self.initialized.load(Ordering::Relaxed) {
                return unsafe { &*self.value.get() }.as_ref().unwrap();
            }
            let value = factory.take().unwrap()();
            unsafe {
                *self.value.get() = Some(value);
            }
            self.initialized.store(true, Ordering::Release);
        }
        unsafe { &*self.value.get() }.as_ref().unwrap()
    }
}

impl<V: Sync> Debug for Lazy<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lazy")
            .field("initialized", &self.initialized)
            .field("value", &self.value)
            .finish()
    }
}

unsafe impl<V: Send + Sync> Send for Lazy<V> {}
unsafe impl<V: Sync> Sync for Lazy<V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy() {
        let lazy = Lazy::new(|| 42);
        assert_eq!(*lazy, 42);
    }
}
