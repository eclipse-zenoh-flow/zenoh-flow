use async_std::sync::Arc;
use async_std::task::sleep;
use futures::future;
use std::time::Duration;
use opencv::{
    core,
    prelude::*,
    videoio,
    highgui,
};
//use async_std::channel::{unbounded, Sender, Receiver};


#[async_std::main]
async fn main() {
    let window_name = &format!("Video-Sink");
        highgui::named_window(window_name, 1).unwrap();

    let mut camera = videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap();  // 0 is the default camera
    let opened = videoio::VideoCapture::is_opened(&camera).unwrap();
    if !opened {
            panic!("Unable to open default camera!");
        }
        let mut encode_options = opencv::types::VectorOfi32::new();
        encode_options.push(opencv::imgcodecs::IMWRITE_JPEG_QUALITY);
        encode_options.push(90);
    loop {
        let mut frame = core::Mat::default();
        camera.read(&mut frame).unwrap();

        let mut reduced = Mat::default();
        opencv::imgproc::resize(&frame, &mut reduced, opencv::core::Size::new(800, 600), 0.0, 0.0 , opencv::imgproc::INTER_LINEAR).unwrap();

        let mut buf = opencv::types::VectorOfu8::new();
        opencv::imgcodecs::imencode(".jpeg", &reduced, &mut buf, &encode_options).unwrap();

        let data =  buf.to_vec();

        let decoded = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data),
            opencv::imgcodecs::IMREAD_COLOR).unwrap();

        if decoded.size().unwrap().width > 0 {
            highgui::imshow(window_name, &decoded).unwrap();
        }

        highgui::wait_key(10).unwrap();

    }




}
