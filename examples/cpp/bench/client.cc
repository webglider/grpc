
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <gflags/gflags.h>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "bench.grpc.pb.h"


using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using midhul::bench::Request;
using midhul::bench::Response;
using midhul::bench::BenchService;


DEFINE_int32(payload_length, 4096, "Length of response payload in bytes");
DEFINE_int32(outstanding_rpcs, 100, "Maximum number of outstanding RPCs per sender thread");
DEFINE_int32(target_rpcs, 1000000, "Target number of RPCs to be sent");
DEFINE_string(server_host, "0.0.0.0", "Server IP address");
DEFINE_string(server_port, "50051", "Server port");

class Semaphore
{
private:
    std::mutex mutex_;
    std::condition_variable condition_;
    unsigned long count_ = 0; // Initialized as locked.

public:
    explicit Semaphore(int capacity) : count_(capacity) {}

    void notify() {
        std::lock_guard<decltype(mutex_)> lock(mutex_);
        ++count_;
        condition_.notify_one();
    }

    void wait() {
        std::unique_lock<decltype(mutex_)> lock(mutex_);
        while(!count_) // Handle spurious wake-ups.
            condition_.wait(lock);
        --count_;
    }

    bool try_wait() {
        std::lock_guard<decltype(mutex_)> lock(mutex_);
        if(count_) {
            --count_;
            return true;
        }
        return false;
    }
};

class BenchClientSender;

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

    // Sender object that created this call
    BenchClientSender *sender;
};

class BenchClientSender {
  public:
    explicit BenchClientSender(std::shared_ptr<Channel> channel, CompletionQueue *cq, int target_rpcs, int max_outstanding, int payload_length)
            : stub_(BenchService::NewStub(channel)), sent_requests_(0), 
            max_outstanding_(max_outstanding), target_rpcs_(target_rpcs),
            payload_length_(payload_length), sem_(max_outstanding), cq_(cq) {}

    // Assembles the client's payload and sends it to the server.
    void SendRequest(const std::string& key, int resp_len) {
        // Data we are sending to the server.
        Request request;
        request.set_key(key);
        request.set_payload_size(resp_len);

        // Call object to store rpc data
        AsyncClientCall* call = new AsyncClientCall;
        call->sender = this;

        // stub_->PrepareAsyncSayHello() creates an RPC object, returning
        // an instance to store in "call" but does not actually start the RPC
        // Because we are using the asynchronous API, we need to hold on to
        // the "call" instance in order to get updates on the ongoing RPC.
        call->response_reader =
            stub_->PrepareAsyncUnaryCall(&call->context, request, cq_);

        // StartCall initiates the RPC call
        call->response_reader->StartCall();

        // Request that, upon completion of the RPC, "reply" be updated with the
        // server's response; "status" with the indication of whether the operation
        // was successful. Tag the request with the memory address of the call object.
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);

    }

    // Notify completion of RPC
    void NotifyCompletion()
    {
        sem_.notify();
    }

    void SendLoop() {

        // Keep sending RPCs until we reach the target
        while(sent_requests_ < target_rpcs_)
        {
            sem_.wait();
            SendRequest("midhul", payload_length_);
            sent_requests_ += 1;
        }

    }

  private:

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<BenchService::Stub> stub_;

    int sent_requests_;

    int max_outstanding_;

    int target_rpcs_;

    int payload_length_;

    Semaphore sem_;

    CompletionQueue *cq_;
};

class BenchClientPoller
{

public:
    explicit BenchClientPoller(int target_responses, int payload_length) 
    : target_responses_(target_responses), payload_length_(payload_length) {}

    // Loop while listening for completed responses.
    // Runs until target responses have been received
    void PollCompletionQueue() {
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
            {
                GPR_ASSERT(call->reply.payload().size() == payload_length_);
                success_responses_ += 1;
            }
            else
            {
                std::cout << "RPC failed: " << call->status.error_code() << " " << call->status.error_message() << std::endl;
            }
            

            // Notify corresponding sender
            call->sender->NotifyCompletion();

            // Once we're complete, deallocate the call object.
            delete call;

            if(total_responses_ == target_responses_)
            {
                break;
            }
        }
    }

    // Explosing the cq
    CompletionQueue *GetCompletionQueue()
    {
        return &cq_;
    }

    int GetNumSuccessfulResponses()
    {
        return success_responses_;
    }

private:

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;

    int total_responses_;

    int success_responses_;

    int target_responses_;

    int payload_length_;

};

int main(int argc, char** argv) {

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::string server_addr = FLAGS_server_host + ":" + FLAGS_server_port;

    // Create poller
    BenchClientPoller poller(FLAGS_target_rpcs, FLAGS_payload_length);

    BenchClientSender sender(grpc::CreateChannel(
            server_addr, grpc::InsecureChannelCredentials()), 
            poller.GetCompletionQueue(), 
            FLAGS_target_rpcs, 
            FLAGS_outstanding_rpcs, 
            FLAGS_payload_length);

    // Spawn poller thread
    std::thread poller_thread_ = std::thread(&BenchClientPoller::PollCompletionQueue, &poller);

    auto t1 = std::chrono::high_resolution_clock::now();

    // Spawn sender thread
    std::thread sender_thread_ = std::thread(&BenchClientSender::SendLoop, &sender);

    // wait for sender and poller ot exit
    sender_thread_.join();
    poller_thread_.join();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> time_taken = t2 - t1;
    double secs = time_taken.count();

    std::cout << "Throughput: " << ((double)FLAGS_target_rpcs/secs) << " rpcs/sec" << std::endl;
    std::cout << "# Successful responses: " << poller.GetNumSuccessfulResponses() << std::endl;


    return 0;
}
