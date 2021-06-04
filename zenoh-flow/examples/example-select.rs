use async_std::sync::Arc;
use async_std::task::sleep;
use futures::future;
use std::time::Duration;
use zenoh_flow::link::{link, ZFLinkSender};
//use async_std::channel::{unbounded, Sender, Receiver};

async fn send<T: Clone>(sender: ZFLinkSender<T>, interveal: Duration, data: T) {
    loop {
        sender.send(Arc::new(data.clone())).await;
        sleep(interveal).await;
    }
}

#[async_std::main]
async fn main() {
    let (s1, mut r1) = link::<u8>(10, 0);
    let (s2, mut r2) = link::<u8>(10, 1);
    let (s3, mut r3) = link::<u8>(10, 2);
    let (s4, mut r4) = link::<u8>(10, 3);

    let _h1 = async_std::task::spawn(async move {
        send(s1, Duration::from_secs(1), 0u8).await;
    });

    let _h2 = async_std::task::spawn(async move {
        send(s2, Duration::from_millis(250), 1u8).await;
    });

    let _h3 = async_std::task::spawn(async move {
        send(s3, Duration::from_millis(750), 2u8).await;
    });

    let _h4 = async_std::task::spawn(async move {
        send(s4, Duration::from_millis(500), 3u8).await;
    });

    loop {
        let mut futs = vec![r1.recv(), r2.recv(), r3.recv(), r4.recv()];
        while !futs.is_empty() {
            match future::select_all(futs).await {
                //this could be "slow" as suggested by LC
                (Ok(v), _i, remaining) => {
                    println!("Link n. {:?} has terminated with {:?}", v.0, v.1);
                    futs = remaining;
                }
                (Err(e), i, remaining) => {
                    println!("Link index {:?} has got error {:?}", i, e);
                    futs = remaining;
                }
            }
        }
        println!("All link recv terminated, restarting...")
    }
}
