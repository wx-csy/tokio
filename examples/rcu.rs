use std::error::Error;
use std::sync::Arc;
use tokio::sync::Rcu;
use rand::random;

#[derive(Debug)]
struct TestStruct(i32);

impl Drop for TestStruct {
    fn drop(&mut self) {
        // prevent compiler optimization
        *std::convert::identity(&mut self.0) = -1;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let test_data = Arc::new(unsafe { Rcu::new(TestStruct(3)) });
    let handles: Vec<_> = (0..32)
        .map(|_| {
            let test_data = test_data.clone();
            tokio::spawn(async move {
                for j in 0usize..10000000usize {
                    if random::<u32>() % 10000 == 0 {
                        test_data.update(TestStruct(random::<u8>() as i32)).get().await;
                    } else {
                        let data = (*test_data.read()).0;
                        assert_ne!(data, -1);
                    }
                    if j & 0xfff == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
    Ok(())
}
