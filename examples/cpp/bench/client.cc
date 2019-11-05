
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <thread>

#include "bench.grpc.pb.h"


using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using midhul::bench::Request;
using midhul::bench::Response;
using midhul::bench::BenchService;

const int c_payload_len = 4096;
const int c_outstanding_rpcs = 100;
const int c_target_rpcs = 5000000;

class BenchClient {
  public:
    explicit BenchClient(std::shared_ptr<Channel> channel)
            : stub_(BenchService::NewStub(channel)), sent_requests_(0), 
            total_responses_(0), success_responses_(0) {}

    // Assembles the client's payload and sends it to the server.
    void SendRequest(const std::string& key, int resp_len) {
        // Data we are sending to the server.
        Request request;
        request.set_key(key);
        request.set_payload_size(resp_len);

        // Call object to store rpc data
        AsyncClientCall* call = new AsyncClientCall;

        // stub_->PrepareAsyncSayHello() creates an RPC object, returning
        // an instance to store in "call" but does not actually start the RPC
        // Because we are using the asynchronous API, we need to hold on to
        // the "call" instance in order to get updates on the ongoing RPC.
        call->response_reader =
            stub_->PrepareAsyncUnaryCall(&call->context, request, &cq_);

        // StartCall initiates the RPC call
        call->response_reader->StartCall();

        // Request that, upon completion of the RPC, "reply" be updated with the
        // server's response; "status" with the indication of whether the operation
        // was successful. Tag the request with the memory address of the call object.
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);

        sent_requests_ += 1;

    }

    // Loop while listening for completed responses.
    // Runs untill target responses have been received
    void AsyncCompleteRpc(int target) {
        void* got_tag;
        bool ok = false;

        // Block until the next result is available in the completion queue "cq".
        while (cq_.Next(&got_tag, &ok)) {
            // The tag in this example is the memory location of the call object
            AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            GPR_ASSERT(ok);

            total_responses_ += 1;
            if (call->status.ok())
                success_responses_ += 1;

            // Once we're complete, deallocate the call object.
            delete call;

            if(sent_requests_ < target)
            {
                // Send another request
                SendRequest("midhul", c_payload_len);
            }

            if(total_responses_ == target)
            {
                break;
            }
        }
    }

  private:

    // struct for keeping state and data information
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        Response reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;


        std::unique_ptr<ClientAsyncResponseReader<Response>> response_reader;
    };

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<BenchService::Stub> stub_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;

    int total_responses_;

    int success_responses_;

    int sent_requests_;
};

int main(int argc, char** argv) {


    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).
    BenchClient bclient(grpc::CreateChannel(
            "localhost:50051", grpc::InsecureChannelCredentials()));

    // Spawn reader thread that loops indefinitely
    // std::thread thread_ = std::thread(&BenchClient::AsyncCompleteRpc, &bclient);

    auto t1 = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < c_outstanding_rpcs; i++) {
        bclient.SendRequest("midhul", c_payload_len);  // The actual RPC call!
    }

    // std::cout << "Press control-c to quit" << std::endl << std::endl;
    // thread_.join();  //blocks forever


    bclient.AsyncCompleteRpc(c_target_rpcs);

    auto t2 = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> time_taken = t2 - t1;
    double secs = time_taken.count();

    std::cout << "Throughput: " << ((double)c_target_rpcs/secs) << " rpcs/sec" << std::endl;


    return 0;
}
