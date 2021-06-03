use futures::future;
use std::time::Duration;
use async_std::task::sleep;
use async_std::channel::{unbounded, Sender, Receiver};



async fn send<T : Clone> (sender : Sender<T>, interveal : Duration, data : T) {
    loop {
        sender.send(data.clone()).await;
        sleep(interveal).await;
    }
}



#[async_std::main]
async fn main() {


    let (s1, r1) = unbounded::<(u8, u8)>();
    let (s2, r2) = unbounded::<(u8, u8)>();
    let (s3, r3) = unbounded::<(u8, u8)>();
    let (s4, r4) = unbounded::<(u8, u8)>();


    let _h1 = async_std::task::spawn(async move {
        send(s1, Duration::from_secs(1), (0u8,0u8)).await;
    });

    let _h2 = async_std::task::spawn(async move {
        send(s2, Duration::from_millis(250), (1u8,1u8)).await;
    });

    let _h3 = async_std::task::spawn(async move {
        send(s3, Duration::from_millis(750), (2u8,2u8)).await;
    });

    let _h4 = async_std::task::spawn(async move {
        send(s4, Duration::from_millis(500), (3u8,3u8)).await;
    });



    loop {
        let mut futs = vec![r1.recv(), r2.recv(), r3.recv(), r4.recv()];
        while !futs.is_empty() {
            match future::select_all(futs).await {
                (Ok(v), _i, remaining) => {
                    println!("Future n. {:?} has terminated with {:?}", v.0, v.1);
                    futs = remaining;
                },
                (Err(e), i, remaining) => {
                    println!("Future index {:?} has got error {:?}", i, e);
                    futs = remaining;
                },
            }
        }
        println!("All futures terminated, restarting...")
    }




}