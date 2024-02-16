using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using UnityEngine;

class Copy : MonoBehaviour
{

    [Serializable]
    public class threadHandle
    {
        IConsumer<Ignore, string> consumer;
        ConsumerConfig config;

        public readonly ConcurrentQueue<string> _queue = new ConcurrentQueue<string>();
        public threadHandle(string brokerid, string groupid, string topic)
        {
            Debug.Log("Kafka - Starting Thread..");
            try
            {
                config = new ConsumerConfig
                {
                    GroupId = groupid,
                    BootstrapServers = brokerid,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true,
                    EnableAutoOffsetStore = false
                };

                /*                var config2 = new ConsumerConfig
                                {
                                    BootstrapServers = "175.45.193.145:3000",
                                    GroupId = "my-group",
                                    EnableAutoCommit = true,
                                    EnableAutoOffsetStore = false,
                                    StatisticsIntervalMs = 5000,
                                    SessionTimeoutMs = 6000,
                                    AutoOffsetReset = AutoOffsetReset.Latest,
                                    EnablePartitionEof = true,
                                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
                                };*/

                Debug.Log("Kafka - Created config");

                consumer = new ConsumerBuilder<Ignore, string>(config)
                .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Debug.Log($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Debug.Log(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Debug.Log(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    Debug.Log($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build();

                consumer.Subscribe(topic);

                if (consumer != null)
                {
                    Debug.Log("Kafka - Subscribed. " + consumer.Name);
                }
                else
                {
                    Debug.Log("Kafka - Fail to Subscribed.");
                }

                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    Debug.Log("On loading");

                    while (cts.IsCancellationRequested)
                    {
                        try
                        {
                            Debug.Log("Wait for message...");
                            // ConsumeResult<Ignore, string> cr = consumer.Consume(TimeSpan.FromMilliseconds(5000));
                            var consumeResult = consumer.Consume(cts.Token);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Debug.Log(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            Debug.Log($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                            try
                            {
                                consumer.StoreOffset(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Debug.LogError($"Store Offset error: {e.Error.Reason}");
                            }

                            var message = consumeResult.Message.Value;
                            // string _message = Encoding.UTF8.GetString(message);
                            _queue.Enqueue(message);

                            JObject json;

                            try
                            {
                                json = JObject.Parse(message);

                                string valuept = json["pt"]?.ToString();
                                if (!string.IsNullOrEmpty(valuept))
                                {
                                    Debug.Log(valuept);
                                }
                                else
                                {
                                    Debug.Log("No pt data available.");
                                }

                                string valuecsv = json["csv"]?.ToString();
                                if (!string.IsNullOrEmpty(valuecsv))
                                {
                                    Debug.Log(valuecsv);
                                }
                                else
                                {
                                    Debug.Log("No csv data available.");
                                }
                            }
                            catch (JsonReaderException ex)
                            {
                                Debug.LogError("Error parsing JSON: " + ex.Message);
                                Debug.LogError("JSON string: " + message);
                            }
                            consumer.Commit(consumeResult);
                        }
                        catch (ConsumeException e)
                        {
                            Debug.LogError("Kafka - Error occured: " + e.Error.Reason);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Debug.Log("Kafka - Canceled..");
                }
            }
            catch (Exception ex)
            {
                Debug.Log("Kafka - Received Expection: " + ex.Message + " trace: " + ex.StackTrace);
                consumer.Unsubscribe();
                consumer.Close();
            }
        }

        public void Run_ManualAssign()
        {
            var config = new ConsumerConfig
            {
                GroupId = "my-group",
                BootstrapServers = "175.45.193.145:3000",
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };

            using (var consumer =
                new ConsumerBuilder<Ignore, string>(config)
                    .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}"))
                    .Build())
            {

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                var topics = new List<string> { "my-json-array-topic" };
                var topicPartitions = new List<TopicPartitionOffset>();
                foreach (var topic in topics)
                {
                    topicPartitions.Add(new TopicPartitionOffset(topic, 0, Offset.Beginning));
                }

                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            Debug.Log($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Debug.LogError($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Debug.Log("Closing consumer.");
                    consumer.Close();
                }
            }
        }
    }
    public void RunKafka()
    {
        print("Run Kafka");
        threadHandle kafka = new threadHandle("175.45.193.145:3000", "my-group", "my-json-array-topic");

        /*kafkaThread = new Thread();

        kafkaThread.Start();
        kafkaStarted = true;*/
    }
    public bool kafkaStarted = false;
    Thread kafkaThread;
    threadHandle _handle;
    public enum Mode
    {
        subscribe,
        manual
    }
    Mode mode;

    void Start()
    {
        mode = Mode.subscribe;

        switch (mode)
        {
            case Mode.subscribe:
                print("subscribe");
                //StartKafkaThread();
                RunKafka();
                break;
            case Mode.manual:
                //Run_ManualAssign();
                break;
            default:
                break;
        }
    }

    void OnDisable()
    {
        StopKafkaThread();
    }
    void OnApplicationQuit()
    {
        StopKafkaThread();
    }

    public void StartKafkaThread()
    {
        /*if (kafkaStarted) return;

        Debug.Log("StartKafkaThread");

        _handle = new threadHandle();
        kafkaThread = new Thread(_handle.StartKafkaListener);

        kafkaThread.Start();
        kafkaStarted = true;
        //StartKafkaListener(config);*/
    }
    private void ProcessKafkaMessage()
    {
        if (kafkaStarted)
        {
            string message;
            while (_handle._queue.TryDequeue(out message))
            {
                Debug.Log(message);
            }
        }
    }

    void StopKafkaThread()
    {
        if (kafkaStarted)
        {
            Debug.Log("StopKafkaThread");

            kafkaThread.Abort();
            kafkaThread.Join();
            kafkaStarted = false;
        }
    }
}

