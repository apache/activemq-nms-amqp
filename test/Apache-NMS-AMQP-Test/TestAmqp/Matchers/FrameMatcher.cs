/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using Amqp.Types;
using NUnit.Framework;

namespace NMS.AMQP.Test.TestAmqp.Matchers
{
    public class FrameMatcher<T> : IFrameMatcher where T : DescribedList
    {
        private bool shouldContinue = true;
        private readonly List<Action<FrameContext<T>>> onCompleteActions = new List<Action<FrameContext<T>>>();
        private readonly List<Action<T, Amqp.Message>> assertions = new List<Action<T, Amqp.Message>>();
        public bool OnFrame(Stream stream, ushort channel, DescribedList describedList, Amqp.Message message)
        {
            Assert.IsNotNull(describedList);
            Assert.IsInstanceOf<T>(describedList, $"Wrong frame! Expected: {typeof(T).Name} but received: {describedList.GetType().Name}");

            T command = (T) describedList;

            foreach (var assertion in assertions)
                assertion(command, message);

            var frameContext = new FrameContext<T>(stream, channel, command);
            foreach (var onCompleteAction in onCompleteActions)
            {
                onCompleteAction.Invoke(frameContext);
            }

            return shouldContinue;
        }

        public FrameMatcher<T> WithAssertion(Action<T> assertion)
        {
            assertions.Add((command, message) =>  assertion(command));
            return this;
        }

        public FrameMatcher<T> WithAssertion(Action<Amqp.Message> assertion)
        {
            assertions.Add((command, message) => assertion(message));
            return this;            
        }

        public FrameMatcher<T> WithAssertion(Action<T, Amqp.Message> assertion)
        {
            assertions.Add(assertion);
            return this;            
        }

        public FrameMatcher<T> WithShouldContinue(bool shouldContinue)
        {
            this.shouldContinue = shouldContinue;
            return this;
        }

        public FrameMatcher<T> WithOnComplete(Action<FrameContext<T>> onComplete)
        {
            this.onCompleteActions.Add(onComplete);
            return this;
        }
        
        IMatcher IMatcher.WithOnComplete(Action<FrameContext> onComplete)
        {
            return this.WithOnComplete(onComplete);            
        }

        public override string ToString()
        {
            return $"FrameMatcher: {typeof(T).Name}";
        }
    }
}