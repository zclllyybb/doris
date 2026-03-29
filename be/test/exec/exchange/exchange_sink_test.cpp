// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/exchange/exchange_sink_test.h"

#include <gtest/gtest.h>

#include <memory>

#include "exec/operator/exchange_sink_buffer.h"

namespace doris {

TEST_F(ExchangeSinkTest, test_normal_end) {
    {
        auto state = std::make_shared<MockRuntimeState>();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);
        auto sink2 = create_sink(state, buffer);
        auto sink3 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, true), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, true), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, true), Status::OK());

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->running_sink_count, 3) << "id : " << id;
        }

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->rpc_channel_is_turn_off, false) << "id : " << id;
        }

        pop_block(dest_ins_id_1, PopState::accept);
        pop_block(dest_ins_id_1, PopState::accept);
        pop_block(dest_ins_id_1, PopState::accept);

        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);

        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->running_sink_count, 0) << "id : " << id;
        }

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->rpc_channel_is_turn_off, true) << "id : " << id;
        }
        clear_all_done();
    }
}

TEST_F(ExchangeSinkTest, test_eof_end) {
    {
        auto state = std::make_shared<MockRuntimeState>();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);
        auto sink2 = create_sink(state, buffer);
        auto sink3 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, true), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, false), Status::OK());

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->running_sink_count, 3) << "id : " << id;
        }

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->rpc_channel_is_turn_off, false) << "id : " << id;
        }

        pop_block(dest_ins_id_1, PopState::eof);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_1]->rpc_channel_is_turn_off, true);
        EXPECT_TRUE(buffer->_rpc_instances[dest_ins_id_1]->package_queue.empty());

        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_2, PopState::accept);

        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);

        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_1]->rpc_channel_is_turn_off, true);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_2]->rpc_channel_is_turn_off, false)
                << "not all eos";
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_3]->rpc_channel_is_turn_off, false)
                << " not all eos";

        EXPECT_TRUE(sink1.add_block(dest_ins_id_1, true).is<ErrorCode::END_OF_FILE>());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, true), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, true), Status::OK());
        pop_block(dest_ins_id_2, PopState::accept);
        pop_block(dest_ins_id_3, PopState::accept);

        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_1]->rpc_channel_is_turn_off, true);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_2]->rpc_channel_is_turn_off, true);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_3]->rpc_channel_is_turn_off, false);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_3]->running_sink_count, 1);

        clear_all_done();
    }
}

TEST_F(ExchangeSinkTest, test_error_end) {
    {
        auto state = std::make_shared<MockRuntimeState>();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);
        auto sink2 = create_sink(state, buffer);
        auto sink3 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, false), Status::OK());

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->running_sink_count, 3) << "id : " << id;
        }

        for (const auto& [id, instance] : buffer->_rpc_instances) {
            EXPECT_EQ(instance->rpc_channel_is_turn_off, false) << "id : " << id;
        }

        pop_block(dest_ins_id_2, PopState::error);

        auto orgin_queue_1_size = done_map[dest_ins_id_1].size();
        auto orgin_queue_2_size = done_map[dest_ins_id_2].size();
        auto orgin_queue_3_size = done_map[dest_ins_id_3].size();

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink2.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink2.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(sink3.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink3.add_block(dest_ins_id_3, false), Status::OK());

        EXPECT_EQ(orgin_queue_1_size, done_map[dest_ins_id_1].size());
        EXPECT_EQ(orgin_queue_2_size, done_map[dest_ins_id_2].size());
        EXPECT_EQ(orgin_queue_3_size, done_map[dest_ins_id_3].size());

        clear_all_done();
    }
}

TEST_F(ExchangeSinkTest, test_queue_size) {
    {
        auto state = std::make_shared<MockRuntimeState>();
        auto buffer = create_buffer(state);

        auto sink1 = create_sink(state, buffer);

        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_1, false), Status::OK());

        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_2, false), Status::OK());

        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());
        EXPECT_EQ(sink1.add_block(dest_ins_id_3, false), Status::OK());

        std::cout << "queue size : " << buffer->_total_queue_size << "\n";

        EXPECT_EQ(buffer->_total_queue_size, 6);

        std::cout << "each queue size : \n" << buffer->debug_each_instance_queue_size() << "\n";

        pop_block(dest_ins_id_2, PopState::eof);

        std::cout << "queue size : " << buffer->_total_queue_size << "\n";

        EXPECT_EQ(buffer->_total_queue_size, 4);

        std::cout << "each queue size : \n" << buffer->debug_each_instance_queue_size() << "\n";

        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_1]->rpc_channel_is_turn_off, false);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_2]->rpc_channel_is_turn_off, true);
        EXPECT_EQ(buffer->_rpc_instances[dest_ins_id_3]->rpc_channel_is_turn_off, false);
        clear_all_done();
    }
}

// Callback that records the state of response_ and cntl_ at the moment call() is invoked,
// then mutates them (simulating callback reuse triggering a new RPC). This lets us verify:
// 1. call() was invoked
// 2. The callback saw the correct original state (before any mutation)
// 3. After call(), the shared objects are mutated (so any code reading them after call()
//    would see wrong values — this is the bug the fix prevents)
template <typename Response>
class StateCapturingCallback : public DummyBrpcCallback<Response> {
    ENABLE_FACTORY_CREATOR(StateCapturingCallback);

public:
    StateCapturingCallback() = default;

    enum class MutateAction {
        WRITE_ERROR,  // Write an error status into response
        CLEAR_STATUS, // Clear the status field
        RESET_CNTL,   // Call cntl_->Reset()
    };

    void set_mutate_action(MutateAction action) { _action = action; }

    void call() override {
        call_invoked = true;
        // Capture state BEFORE mutation — this is what call() sees.
        cntl_failed_at_call_time = this->cntl_->Failed();
        if (this->cntl_->Failed()) {
            cntl_error_at_call_time = this->cntl_->ErrorText();
        }
        response_status_at_call_time = Status::create(this->response_->status());

        // Now mutate (simulating callback reuse / new RPC)
        switch (_action) {
        case MutateAction::WRITE_ERROR: {
            Status err = Status::InternalError("injected by callback reuse");
            err.to_protobuf(this->response_->mutable_status());
            break;
        }
        case MutateAction::CLEAR_STATUS: {
            this->response_->mutable_status()->set_status_code(0);
            this->response_->mutable_status()->clear_error_msgs();
            break;
        }
        case MutateAction::RESET_CNTL: {
            this->cntl_->Reset();
            break;
        }
        }
    }

    // Observable state
    bool call_invoked = false;
    bool cntl_failed_at_call_time = false;
    std::string cntl_error_at_call_time;
    Status response_status_at_call_time;

private:
    MutateAction _action = MutateAction::WRITE_ERROR;
};

using TestCallback = StateCapturingCallback<PTransmitDataResult>;

// Test: Response starts OK. call() writes an error into it.
// With correct ordering (log-before-call): the closure's logging sees OK (no warning),
// then call() runs and the callback captures the OK status at call time.
// With WRONG ordering (call-before-log): call() writes error first, then the closure
// would log the error — a false positive. We verify call() saw OK at invocation time,
// proving it ran after (or at least not before) the status was checked by the closure.
TEST_F(ExchangeSinkTest, test_closure_call_sees_original_ok_response) {
    auto callback = TestCallback::create_shared();
    // Response starts OK (default).
    callback->set_mutate_action(TestCallback::MutateAction::WRITE_ERROR);

    auto req = std::make_shared<PTransmitDataParams>();
    auto* closure = new AutoReleaseClosure<PTransmitDataParams, TestCallback>(req, callback);

    closure->Run(); // self-deletes

    EXPECT_TRUE(callback->call_invoked) << "call() should have been invoked";
    EXPECT_TRUE(callback->response_status_at_call_time.ok())
            << "call() must see the original OK response status. "
               "If it saw an error, the ordering is wrong.";
    EXPECT_FALSE(callback->cntl_failed_at_call_time);
}

// Test: Response starts with an error. call() clears it.
// With correct ordering: closure logs the error first, then call() runs and sees the error
// at invocation time (before clearing it).
// With WRONG ordering: call() clears the error first, then the closure sees OK — error
// silently swallowed. We verify call() saw the error, proving the closure read it first.
TEST_F(ExchangeSinkTest, test_closure_call_sees_original_error_response) {
    auto callback = TestCallback::create_shared();
    // Set error status on the response BEFORE Run().
    Status err = Status::InternalError("original RPC error");
    err.to_protobuf(callback->response_->mutable_status());
    callback->set_mutate_action(TestCallback::MutateAction::CLEAR_STATUS);

    auto req = std::make_shared<PTransmitDataParams>();
    auto* closure = new AutoReleaseClosure<PTransmitDataParams, TestCallback>(req, callback);

    closure->Run();

    EXPECT_TRUE(callback->call_invoked) << "call() should have been invoked";
    EXPECT_FALSE(callback->response_status_at_call_time.ok())
            << "call() must see the original error status before clearing it. "
               "If it saw OK, the ordering is wrong — call() cleared it before the closure "
               "could log the error, silently swallowing it.";
}

// Test: Controller is failed. call() resets it.
// With correct ordering: closure checks cntl_->Failed() first (true), logs the error,
// then call() runs and sees Failed() == true before resetting.
// With WRONG ordering: call() resets the controller first, then the closure sees
// Failed() == false — RPC failure silently lost. We verify call() saw the failure.
TEST_F(ExchangeSinkTest, test_closure_call_sees_original_rpc_failure) {
    auto callback = TestCallback::create_shared();
    // Mark the controller as failed BEFORE Run().
    callback->cntl_->SetFailed("simulated network timeout");
    callback->set_mutate_action(TestCallback::MutateAction::RESET_CNTL);

    auto req = std::make_shared<PTransmitDataParams>();
    auto* closure = new AutoReleaseClosure<PTransmitDataParams, TestCallback>(req, callback);

    closure->Run();

    EXPECT_TRUE(callback->call_invoked) << "call() should have been invoked";
    EXPECT_TRUE(callback->cntl_failed_at_call_time)
            << "call() must see cntl_->Failed() == true before resetting the controller. "
               "If it saw false, the ordering is wrong — call() reset the controller before "
               "the closure could detect the failure.";
    EXPECT_NE(callback->cntl_error_at_call_time.find("simulated network timeout"),
              std::string::npos)
            << "call() must see the original error text";
}

} // namespace doris
