using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TMPro;
using UnityEngine;

class GetKafkaMessage : MonoBehaviour
{
    [Serializable]
    public class threadHandle
    {
        /*private IConsumer<Ignore, string> consumer;
        private const string kafkaBroker = "175.45.193.145:3000";
        private const string kafkaTopic = "my-json-array-topic";
        ConsumerConfig config;

        void Start()
        {
            // Kafka Consumer Configuration
            config = new ConsumerConfig
            {
                BootstrapServers = kafkaBroker,
                GroupId = "my-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            // Create Kafka Consumer instance
            consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            // Subscribe to Kafka topic
            consumer.Subscribe(kafkaTopic);

            // Start a new thread to continuously poll for messages
            ThreadPool.QueueUserWorkItem(_ =>
            {
                while (true)
                {
                    ConsumeMessage();
                }
            });

            if (config != null)
            {
                Debug.Log("Set config.");
            }
        }

        void ConsumeMessage()
        {
            Debug.Log("Consume Message");
            try
            {
                var consumeResult = consumer.Consume();

                if (consumeResult != null && consumeResult.Message != null)
                {
                    var message = Encoding.UTF8.GetBytes(consumeResult.Message.Value);
                    string _message = Encoding.UTF8.GetString(message);
                    Debug.Log($"Received message: {_message}");
                }
            }
            catch (ConsumeException e)
            {
                Debug.LogError($"Error consuming message: {e.Error.Reason}");
            }
        }

        void OnDestroy()
        {
            // Dispose of the Kafka Consumer when the Unity application is closed
            consumer?.Close();
        }*/

        IConsumer<Ignore, string> consumer; // �޼����� string �Ǵ� byte �������� �޾ƿ� ����
        ConsumerConfig config; // ��û �޼��� ���� ����

        public readonly ConcurrentQueue<string> _queue = new ConcurrentQueue<string>(); // ���Լ���(FIFO) ����� ������ ����       
        public threadHandle(string brokerid, string groupid)
        {
            Debug.Log("Kafka - Starting Thread..");
            try
            {
                config = new ConsumerConfig
                {
                    GroupId = "",
                    //+DateTime.Now,  // unique group, so each listener gets all messages
                    BootstrapServers = "211.193.31.69:9092",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true,
                    EnableAutoOffsetStore = false
                };

                var config2 = new ConsumerConfig
                {
                    BootstrapServers = brokerid,
                    GroupId = groupid,
                    EnableAutoCommit = false, // �ڵ� Ŀ��: 5��(�⺻��)���� �����Ӱ� �����͸� ȣ�� �� �� ���� ������ �������� Ŀ��
                                              // Ŀ��: �� ��Ƽ�ǿ� ���� ���� ��ġ�� ������Ʈ �ϴ� ����
                    EnableAutoOffsetStore = true, // ������ ���� �ڵ� ����
                                                  // ������: consumer�� kafka���� ������ �޼����� ��ġ ����
                                                  //       ��Ƽ�� �� �޼����� ��ġ
                                                  // topic: ������ ���� �����
                    StatisticsIntervalMs = 3000, // ������ �������� kafka consumer�� ��� �����͸� json �������� ����
                    SessionTimeoutMs = 6000, // ���� Ÿ�Ӿƿ� �ð� ms ����                    
                    AutoOffsetReset = AutoOffsetReset.Earliest, // latest: ���� �ʱ⿡ topic�� �� consumer offset ����
                    EnablePartitionEof = true, // consumeResult ��ü���� ���� �޼����� partition�� ���� �����ߴ��� Ȯ�� �� �߰� ����� ���� Ȱ��ȭ
                    // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
                    // ��Ƽ�� �Ҵ� ���� sticky: consumer�� partition�� �� �� �Ҵ�Ǹ� ������ �� �������� �ش� consumer�� �����Ǿ� ����
                    // CooperativeSticky: consumer �߰�, ���� �� consumer group ��ü ���뷱�� ����, ��Ƽ�� �Ҵ��� ȿ���� ���� 
                };

                // ���뷱��: ������ �׷쿡 ���ο� ������ ���� or ���� �����Ӱ� �ٿ� �� �� �߻�
                // ���� ������ ��Ƽ���� �ƴ� ���ο� ��Ƽ�ǿ� �Ҵ�� 
                // �޼��� ó�� �ð��� Ŀ�� �ֱ⺸�� ũ�� �޼����� �޾ƿ��� ���ϰ� ���� Ŀ�Կ� �ش��ϴ� �޼����� �޾ƿ� �޼��� ���� �߻�
                // �޼��� poll() �� Ŀ�� �ֱ⸦ �� ä���� ���ϰ� ����(������ �ٿ�) �߻� �� Ŀ�� ������ �̹� poll()�ߴ� �޼����� �ޱ� ���� ��ġ�ϹǷ� �޼��� �ߺ� �߻�

                Debug.Log("Kafka - Created config");

                // �޼��� �޾ƿ� ������ ����
                consumer = new ConsumerBuilder<Ignore, string>(config2)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}")) // .consume �ܰ迡�� �߻��ϴ� ���� �ֿܼ� ǥ��
                .SetStatisticsHandler((_, json) => Debug.Log($"Statistics: {json}")) // .comsume �ܰ迡�� �޾ƿ� consumer ���� json�������� �ֿܼ� ǥ��
                .SetPartitionsAssignedHandler((c, partitions) => // ��Ƽ�� �Ҵ� ���� ǥ��
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Debug.Log(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
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
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Debug.Log($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build(); // consumer build
            }
            catch (Exception ex) // ī��ī ���� �Ұ� �� ���� ǥ��
            {
                Debug.Log("Kafka - Received Expection: " + ex.Message + " trace: " + ex.StackTrace);
            }
        }

        public void StartKafkaListner()
        {

            // topic���� �޼��� ���� ��û
            consumer.Subscribe("quickstart-events");

            if (consumer != null)
            {
                Debug.Log("Kafka - Subscribed. " + consumer.Name);
            }
            else
            {
                Debug.Log("Kafka - Fail to Subscribed.");
            }

            // CancellationTokenSource Ŭ����
            // : CancellationToken(��ҿ�û)����, Cancel ��û�� CancellationToken�鿡�� ������ ����
            // CancellationToken ����ü
            // : Cancel ���¸� ����͸��ϴ� ���� Listner�鿡 ���� ���Ǵ� ����ü
            //   �۾��̳� �޼��尡 ����Ǵ� ���� ��� ��û�� �߻��ϸ� �ش� �۾��̳� �޼��尡 ��ҵ� �� �ֵ��� �ϴ� ��Ŀ���� ����
            //   �������� ����
            // IsCancellationRequested : bool Ÿ�� ������, �۾� ��� ���θ� �˷���
            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                Debug.Log("On loading");

                // ��� ��û�� ���� ������ �ݺ� ����
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        Debug.Log("Wait for message...");

                        // ������ consumer ����ҿ� �޼��� �޾ƿ� ��� ����, �޾ƿ��� �Ⱓ ���ڰ��� ����
                        // ConsumeResult<Ignore, string> consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(5000));
                        var consumeResult = consumer.Consume(cts.Token);     // none : ��� ��û�� ������� ���μ����� ����                           

                        if (consumeResult.IsPartitionEOF) // consumeResult ��ü���� ���� �޼����� partition�� ���� �����ߴٸ�
                        {
                            Debug.Log(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }
                        // Debug.Log("Consume result: " + consumeResult.Message.Value);
                        // ���������� consumer �޼����� �޾ƿ��� �� topic �̸�, partition ��ȣ, ������ ��� json�����Ͱ� ǥ��
                        if (consumeResult != null)
                            Debug.Log($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                        else Debug.Log("No data available.");

                        try
                        {
                            // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                            // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                            // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                            // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                            consumer.StoreOffset(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Debug.LogError($"Store Offset error: {e.Error.Reason}");
                        }

                        // Got message! Decode and put on queue
                        var message = consumeResult.Message.Value; // �޾ƿ� �޼����� string �������� ����
                                                                   //string _message = Encoding.UTF8.GetString(message);

                        // message queue�� �޾ƿ� string �� �߰�
                        _queue.Enqueue(message);
                        // ���Լ���

                        JObject json;

                        try
                        {
                            // �޼����� json ���·� �Ľ�
                            json = JObject.Parse(message);
                            print(json);

                            string valuept = json["pt"]?.ToString(); // pt ���� �ٿ�ε�
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
                                Debug.Log(valuecsv);         // csv ���� �ٿ�ε�
                            }
                            else
                            {
                                Debug.Log("No csv data available.");
                            }
                        }
                        catch (JsonReaderException ex) // json �Ľ� �Ұ� �� ���� ǥ��
                        {
                            //consumer.Commit();
                            Debug.LogError("Error parsing JSON: " + ex.Message);
                            Debug.LogError("JSON string: " + message);
                        }
                        consumer.Commit(consumeResult); // ���� Ŀ��
                    }
                    catch (ConsumeException e) // consume �Ұ� �� ���� ǥ��
                    {

                        Debug.LogError("Kafka - Error occured: " + e.Error.Reason);
                    }
                }

            }
            catch (OperationCanceledException) // ���μ��� ��� ��û �� ǥ��
            {
                Debug.Log("Kafka - Canceled..");
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }

    }

    public void Run_ManualAssign() // ���� consume �޼��� 
    {
        var config = new ConsumerConfig
        {
            // the group.id property must be specified when creating a consumer, even 
            // if you do not intend to use any consumer group functionality.
            GroupId = "my-group",
            BootstrapServers = "175.45.193.145:3000",
            // partition offsets can be committed to a group even by consumers not
            // subscribed to the group. in this example, auto commit is disabled
            // to prevent this from occurring.
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
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            // List of topics to consume
            var topics = new List<string> { "my-json-array-topic" };
            // Assign each topic with offset set to the beginning
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
                        // Note: End of partition notification has not been enabled, so
                        // it is guaranteed that the ConsumeResult instance corresponds
                        // to a Message, and not a PartitionEOF event.
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

    public bool kafkaStarted = false;
    public TextMeshProUGUI time;
    public TextMeshProUGUI msg;
    Thread kafkaThread;
    threadHandle _handle;
    public enum Mode // �񵿱�/���� ���� ��� ����
    {
        subscribe,
        manual
    }
    Mode mode;

    // ����Ƽ ���� �̺�Ʈ �Լ� start()
    void Start()
    {
        mode = Mode.subscribe;

        switch (mode)
        {
            case Mode.subscribe:
                StartKafkaThread();
                break;
            case Mode.manual:
                Run_ManualAssign();
                break;
            default:
                ProcessKafkaMessage();
                break;
        }
    }
    private void Update()
    {
        time.text = Time.time.ToString();
        ProcessKafkaMessage();
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
        if (kafkaStarted) return;

        Debug.Log("StartKafkaThread");

        _handle = new threadHandle("211.193.31.69:9092", "my-group"); // �ش� ��ũ��Ʈ �ڵ鷯(������) ����
        kafkaThread = new Thread(_handle.StartKafkaListner); // �ڵ鷯�� ���� StartKafkaListner ������ ����


        kafkaThread.Start(); // ī��ī ������ ����
        kafkaStarted = true;
        //StartKafkaListener(config);
    }
    private void ProcessKafkaMessage() // update ���� ���������� ����
    {
        if (kafkaStarted)
        {
            string message;
            while (_handle._queue.TryDequeue(out message)) // �޼���ť���� �޼����� ���õ� �� ���� ����
            {
                Debug.Log(message); // ���� �޼����� �ֿܼ� ���
                msg.text = message;
            }
        }
    }

    void StopKafkaThread() // ���� �����ǰų� ��ũ��Ʈ�� ��Ȱ��ȭ �� ������ ����
    {
        if (kafkaStarted)
        {
            Debug.Log("StopKafkaThread");

            kafkaThread.Abort(); // ī��ī ������ �ߴ�
            kafkaThread.Join();
            kafkaStarted = false;
        }
    }
}

