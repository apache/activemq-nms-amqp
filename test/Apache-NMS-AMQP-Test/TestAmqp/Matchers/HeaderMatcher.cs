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
using NUnit.Framework;

namespace NMS.AMQP.Test.TestAmqp.Matchers
{
    public class HeaderMatcher : IMatcher
    {
        private readonly List<Action<FrameContext>> onCompleteActions = new List<Action<FrameContext>>();
        
        private readonly byte[] expectedHeader;
        private readonly byte[] response;

        public HeaderMatcher(byte[] expectedHeader, byte[] response)
        {
            this.expectedHeader = expectedHeader;
            this.response = response;
        }

        public void OnHeader(Stream stream, byte[] header)
        {
            CollectionAssert.AreEqual(expectedHeader, header, "Header should match");

            if (response != null && response.Length > 0)
            {
                stream.Write(response, 0, response.Length);
            }

            var frameContext = new FrameContext(stream, 0);
            foreach (var completeAction in onCompleteActions) 
                completeAction.Invoke(frameContext);
        }

        public HeaderMatcher WithOnComplete(Action<FrameContext> onComplete)
        {
            this.onCompleteActions.Add(onComplete);
            return this;
        }
        
        IMatcher IMatcher.WithOnComplete(Action<FrameContext> onComplete)
        {
            return this.WithOnComplete(onComplete);            
        }
    }
}