#include <iostream>
#include <memory>
#include <string>
#include <fstream>

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <gflags/gflags.h>

#include "bench.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using midhul::bench::Request;
using midhul::bench::Response;
using midhul::bench::BenchService;

DEFINE_string(host, "0.0.0.0", "Host IP address");
DEFINE_string(port, "50051", "Server port");

// const int c_max_threads = 8;

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

void RunServer(std::string host, std::string port) {
  std::string server_address(host + ":" + port);
  BenchServiceImpl service;

  ServerBuilder builder;
  // grpc::ResourceQuota rq;
  // rq.SetMaxThreads(c_max_threads);
  // builder.SetSyncServerOption(ServerBuilder::SyncServerOption::NUM_CQS, 1);
  // builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MIN_POLLERS, 1);
  // builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MAX_POLLERS, 1);
  // builder.SetResourceQuota(rq);
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

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize dummy bytes with random bytes
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  GPR_ASSERT(urandom);
  urandom.read(reinterpret_cast<char*>(dummy_bytes), c_max_payload_size);
  urandom.close();
  std::cout << "Initialized random bytes" << dummy_bytes[0] << " " << dummy_bytes[1] << std::endl;

  RunServer(FLAGS_host, FLAGS_port);

  return 0;
}
