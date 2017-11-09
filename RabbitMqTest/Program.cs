using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace RabbitMqTest
{
    class Program
    {
        public class RabbitModel : IDisposable
        {
            private static ConnectionFactory rabbitConnectionFactory;

            static RabbitModel()
            {
                rabbitConnectionFactory = new ConnectionFactory
                {
                    UserName = "client",
                    Password = "client",
                    VirtualHost = "/",
                    HostName = "localhost",
                    Port = 5672
                };
            }

            public void CreateExchangeQueueBind()
            {                
                Model.ExchangeDeclare(ExchangeName, ExchangeType.Direct);
                Model.QueueDeclare(QueueName, false, false, false, null);
                Model.QueueBind(QueueName, ExchangeName, RoutingKey, null);
            }

            public IConnection Connection { get; private set; }

            public IModel Model { get; private set; }

            public string ExchangeName { get; private set; }

            public string QueueName { get; private set; }

            public string RoutingKey { get; private set; }

            public RabbitModel(string exchangeName, string queueName, string routingKey)
            {
                ExchangeName = exchangeName;
                QueueName = queueName;
                RoutingKey = routingKey;
                Connection = rabbitConnectionFactory.CreateConnection();
                Model = Connection.CreateModel();
                CreateExchangeQueueBind();
            }

            public virtual void Dispose()
            {
                Model?.Dispose();
                Connection?.Dispose();
            }
        }

        public class RabbitMessage
        {
            public string ExchangeNameOverride { get; set; }

            public string RoutingKeyOverride { get; set; }

            public IDictionary<string, object> Properties { get; set; }

            public byte[] Body { get; set; }

            public string BodyStr
            {
                get
                {
                    if (Body == null)
                        return null;
                    return Encoding.UTF8.GetString(Body);
                }
                set
                {
                    if (value == null)
                        Body = null;
                    else Body = Encoding.UTF8.GetBytes(value);
                }
            }
        }

        public class RabbitMessage<T> : RabbitMessage where T : class, new()
        {
            private JsonSerializer jsonSer = null;

            public T BodyObj
            {
                get
                {
                    if (Body == null)
                        return null;
                    if (jsonSer == null)
                        jsonSer = new JsonSerializer();
                    using (var ms = new MemoryStream(Body))
                    using (var streamReader = new StreamReader(ms, Encoding.UTF8))
                    using (var jsonReader = new JsonTextReader(streamReader))
                        return jsonSer.Deserialize<T>(jsonReader);
                }
                set
                {
                    if (value == null)
                    {
                        Body = null;
                        return;
                    }
                    if (jsonSer == null)
                        jsonSer = new JsonSerializer();
                    using (var ms = new MemoryStream())
                    using (var streamWriter = new StreamWriter(ms, Encoding.UTF8))
                    {
                        jsonSer.Serialize(streamWriter, value);
                        streamWriter.Flush();
                        Body = ms.ToArray();
                    }
                }
            }
        }

        public class RabbitPostBlock<T> : RabbitModel where T : RabbitMessage, new()
        {
            public delegate void ErrorDelegate(Exception exception);

            public event ErrorDelegate Errors;
            
            public ActionBlock<T> PostMessageBlock { get; private set; }

            public RabbitPostBlock(string exchangeName, string queueName, string routingKey, 
                CancellationToken cancelToken = default(CancellationToken)) : base(exchangeName, queueName, routingKey)
            {
                PostMessageBlock = new ActionBlock<T>((Action<T>)PostMessageToRabbit, new ExecutionDataflowBlockOptions
                    { CancellationToken = cancelToken, MaxDegreeOfParallelism = 1 });
            }

            private void PostMessageToRabbit(T message)
            {
                try
                {
                    IBasicProperties props;
                    if (message.Properties != null && message.Properties.Count > 0)
                    {
                        props = Model.CreateBasicProperties();
                        props.ContentType = "text/plain";
                        //props.DeliveryMode = 2;
                        props.Headers = message.Properties;
                    }
                    else props = null;
                    Model.BasicPublish(message.ExchangeNameOverride ?? ExchangeName, 
                        message.RoutingKeyOverride ?? RoutingKey, false, props, message.Body);
                }
                catch(Exception ex)
                {
                    Errors?.Invoke(ex);
                }
            }

            public override void Dispose()
            {
                PostMessageBlock?.Complete();
                PostMessageBlock?.Completion.Wait();
                PostMessageBlock = null;
                base.Dispose();
            }
        }

        public class RabbitRecieveBlock<T> : RabbitModel where T : RabbitMessage, new()
        {
            public delegate void ErrorDelegate(Exception exception);

            public event ErrorDelegate Errors;

            public ITargetBlock<T> ReceiveMessageBlock { get; private set; }

            public Task Running { get; private set; }

            private CancellationToken cancelToken;
            private bool stopSignal = false;

            public RabbitRecieveBlock(string exchangeName, string queueName, string routingKey, ITargetBlock<T> targetBlock, 
                bool startImmediately = true, CancellationToken cancelToken = default(CancellationToken)) : base(exchangeName, queueName, routingKey)
            {
                this.cancelToken = cancelToken;
                ReceiveMessageBlock = targetBlock;
                if (startImmediately)
                    Start();
            }

            public bool Start()
            {
                if (Running == null)
                {
                    Running = ReceiveMessages();
                    return true;
                }
                else return false;
            }

            public async Task<bool> Stop()
            {
                if (Running == null)
                    return true;
                stopSignal = true;
                await Running;
                return true;
            }

            private async Task ReceiveMessages()
            {
                while (!cancelToken.IsCancellationRequested && !stopSignal)
                {
                    try
                    {
                        var result = Model.BasicGet(QueueName, false);
                        if (result == null)
                        {
                            await Task.Yield();
                        }
                        else
                        {
                            var message = new T()
                            {
                                Properties = result.BasicProperties.Headers,
                                Body = result.Body,
                            };
                            while (!await ReceiveMessageBlock.SendAsync(message, cancelToken)) ;
                            Model.BasicAck(result.DeliveryTag, false);
                        }
                    }
                    catch (OperationCanceledException) { return; }
                    catch (Exception ex)
                    {
                        Errors?.Invoke(ex);
                    }
                }
            }

            public override void Dispose()
            {
                try
                {
                    Stop().Wait();
                }
                catch { }
                //ReceiveMessageBlock?.Complete();
                //ReceiveMessageBlock?.Completion.Wait();
                ReceiveMessageBlock = null;
                base.Dispose();
            }
        }

        static void Main(string[] args)
        {
            var queueName = "general_queue";
            var exchangeName = "general_exchange";
            var routingKey = "*";
            var cancelSource = new CancellationTokenSource();
            var poster1 = new RabbitPostBlock<RabbitMessage>(exchangeName, queueName, routingKey, cancelSource.Token);
            var numMessages = 100000;
            int consumeParallelism = 10;
            var messages = new Dictionary<int, Tuple<RabbitMessage, Stopwatch>>();
            for (int i = 0; i < numMessages; i++)
            {
                messages.Add(i, new Tuple<RabbitMessage, Stopwatch>(new RabbitMessage
                {
                    BodyStr = $"Y'ello world {i}",
                    Properties = new Dictionary<string, object>()
                    {
                        { "Index", i }
                    }
                }, new Stopwatch()));
            }            
            int receivedCount = 0;
            var consumeBlock = new ActionBlock<RabbitMessage>(m =>
            {
                //if (m.Properties != null)
                //{
                //    var i = (int)m.Properties["Index"];
                //    messages[i].Item2.Stop();
                //}
                //else Console.WriteLine(m.BodyStr);
                receivedCount++;
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = consumeParallelism });
            var consumers = new List<RabbitRecieveBlock<RabbitMessage>>(consumeParallelism);
            for(int i = 0; i < consumeParallelism; i++)
                consumers.Add(new RabbitRecieveBlock<RabbitMessage>(exchangeName, queueName, routingKey, consumeBlock, true, cancelSource.Token));

            foreach(var msg in messages.Values)
            {
                msg.Item2.Start();
                poster1.PostMessageBlock.Post(msg.Item1);
            }
            while (receivedCount != numMessages)
                Task.Delay(100, cancelSource.Token).Wait();
            poster1.Dispose();

            Task.WhenAll(consumers.Select(c => c.Stop())).Wait();

            consumeBlock.Complete();
            consumeBlock.Completion.Wait();

            double totalTime = 0;
            double minTime = double.MaxValue;
            double maxTime = 0;
            foreach(var msg in messages)
            {
                var elapsed = msg.Value.Item2.Elapsed.TotalMilliseconds;
                totalTime += elapsed;
                minTime = Math.Min(minTime, elapsed);
                maxTime = Math.Max(maxTime, elapsed);
                //Console.WriteLine($"{msg.Key} {msg.Value.Item2.Elapsed}");
            }
            Console.WriteLine($"Total: {totalTime}");
            Console.WriteLine($"Average: {totalTime / numMessages}");
            Console.WriteLine($"Min: {minTime}");
            Console.WriteLine($"Max: {maxTime}");
        }
    }
}
