#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>

#include "bench.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using midhul::bench::Request;
using midhul::bench::Response;
using midhul::bench::BenchService;

const int c_max_threads = 1;

const int c_max_payload_size = 16*1024*1024;
unsigned char dummy_bytes[c_max_payload_size];

// Logic and data behind the server's behavior.
class BenchServiceImpl final : public BenchService::Service {
  Status UnaryCall(ServerContext* context, const Request* request,
                  Response* reply) override {
	  
	  //std::cout << "Got request" << std::endl;
	  reply->set_key(request->key());
    GPR_ASSERT(request->payload_size() < c_max_payload_size);
    reply->set_payload(dummy_bytes, request->payload_size());
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  BenchServiceImpl service;

  ServerBuilder builder;
  grpc::ResourceQuota rq;
  rq.SetMaxThreads(c_max_threads);
  builder.SetSyncServerOption(ServerBuilder::SyncServerOption::NUM_CQS, 1);
  builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MIN_POLLERS, 1);
  builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MAX_POLLERS, 1);
  builder.SetResourceQuota(rq);
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
