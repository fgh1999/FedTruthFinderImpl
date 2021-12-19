fn main() {
    // tonic_build::configure()
    //     // .build_server(false)
    //     .compile(
    //         &["proto/master.proto", "proto/slave.proto"],
    //         &[],
    //     ).unwrap();
    tonic_build::compile_protos("./proto/master.proto").unwrap();
    tonic_build::compile_protos("./proto/slave.proto").unwrap();
}
