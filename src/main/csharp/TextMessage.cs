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


namespace Apache.NMS.Amqp
{
    public class TextMessage : BaseMessage, ITextMessage
    {
        public const int SIZE_OF_INT = 4; // sizeof(int) - though causes unsafe issues with net 1.1

        private String text;

        public TextMessage()
        {
        }

        public TextMessage(String text)
        {
            this.Text = text;
        }


        // Properties

        public string Text
        {
            get
            {
                if(text == null)
                {
                    // now lets read the content
                    byte[] data = this.Content;
                    if(data != null)
                    {
                        // TODO assume that the text is ASCII
                        char[] chars = new char[data.Length - SIZE_OF_INT];
                        for(int i = 0; i < chars.Length; i++)
                        {
                            chars[i] = (char) data[i + SIZE_OF_INT];
                        }
                        text = new String(chars);
                    }
                }
                return text;
            }

            set
            {
                this.text = value;
                byte[] data = null;
                if(text != null)
                {
                    // TODO assume that the text is ASCII

                    byte[] sizePrefix = System.BitConverter.GetBytes(text.Length);
                    data = new byte[text.Length + sizePrefix.Length];  //int at the front of it

                    // add the size prefix
                    for(int j = 0; j < sizePrefix.Length; j++)
                    {
                        // The bytes need to be encoded in big endian
                        if(BitConverter.IsLittleEndian)
                        {
                            data[j] = sizePrefix[sizePrefix.Length - j - 1];
                        }
                        else
                        {
                            data[j] = sizePrefix[j];
                        }
                    }

                    // Add the data.
                    char[] chars = text.ToCharArray();
                    for(int i = 0; i < chars.Length; i++)
                    {
                        data[i + sizePrefix.Length] = (byte) chars[i];
                    }
                }
                this.Content = data;

            }
        }

    }
}

