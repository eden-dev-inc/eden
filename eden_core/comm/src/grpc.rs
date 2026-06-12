// use proto::proto::EndpointConnection;
// use opentelemetry::{
//     global,
//     propagation::{Injector, TextMapPropagator},
//     trace::{Span, Tracer},
//     Context,
// };
// use tonic::{Request, Response, Status};
//
// struct GrpcInjector<'a>(&'a mut Request<EndpointConnection>);
//
// impl<'a> Injector for GrpcInjector<'a> {
//     fn set(&mut self, key: &str, value: String) {
//         self.0.get_mut().trace_context.insert(key.to_string(), value);
//     }
// }
