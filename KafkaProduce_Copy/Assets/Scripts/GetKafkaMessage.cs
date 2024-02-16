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

        IConsumer<Ignore, string> consumer; // 메세지를 string 또는 byte 형식으로 받아올 변수
        ConsumerConfig config; // 요청 메세지 구성 변수

        public readonly ConcurrentQueue<string> _queue = new ConcurrentQueue<string>(); // 선입선출(FIFO) 방식의 데이터 구조       
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
                    EnableAutoCommit = false, // 자동 커밋: 5초(기본값)마다 컨슈머가 데이터를 호출 할 때 가장 마지막 오프셋을 커밋
                                              // 커밋: 각 파티션에 대해 현재 위치를 업데이트 하는 동작
                    EnableAutoOffsetStore = true, // 오프셋 저장 자동 설정
                                                  // 오프셋: consumer가 kafka에서 가져간 메세지의 위치 정보
                                                  //       파티션 내 메세지의 위치
                                                  // topic: 오프셋 정보 저장소
                    StatisticsIntervalMs = 3000, // 지정된 간격으로 kafka consumer의 통계 데이터를 json 형식으로 전달
                    SessionTimeoutMs = 6000, // 세션 타임아웃 시간 ms 단위                    
                    AutoOffsetReset = AutoOffsetReset.Earliest, // latest: 가장 초기에 topic에 들어간 consumer offset 전달
                    EnablePartitionEof = true, // consumeResult 객체에서 현재 메세지가 partition의 끝에 도달했는지 확인 및 추가 명령을 위해 활성화
                    // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
                    // 파티션 할당 전략 sticky: consumer에 partition이 한 번 할당되면 가능한 한 오랫동안 해당 consumer에 고정되어 유지
                    // CooperativeSticky: consumer 추가, 제거 시 consumer group 전체 리밸런싱 방지, 파티션 할당의 효율성 증가 
                };

                // 리밸런싱: 컨슈머 그룹에 새로운 컨슈머 조인 or 기존 컨슈머가 다운 될 때 발생
                // 이전 토픽의 파티션이 아닌 새로운 파티션에 할당됨 
                // 메세지 처리 시간이 커밋 주기보다 크면 메세지를 받아오지 못하고 다음 커밋에 해당하는 메세지를 받아와 메세지 유실 발생
                // 메세지 poll() 후 커밋 주기를 다 채우지 못하고 에러(컨슈머 다운) 발생 시 커밋 정보는 이미 poll()했던 메세지를 받기 전에 위치하므로 메세지 중복 발생

                Debug.Log("Kafka - Created config");

                // 메세지 받아올 컨슈머 생성
                consumer = new ConsumerBuilder<Ignore, string>(config2)
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Debug.LogError($"Error: {e.Reason}")) // .consume 단계에서 발생하는 에러 콘솔에 표시
                .SetStatisticsHandler((_, json) => Debug.Log($"Statistics: {json}")) // .comsume 단계에서 받아온 consumer 정보 json형식으로 콘솔에 표시
                .SetPartitionsAssignedHandler((c, partitions) => // 파티션 할당 정보 표시
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
            catch (Exception ex) // 카프카 연결 불가 시 오류 표시
            {
                Debug.Log("Kafka - Received Expection: " + ex.Message + " trace: " + ex.StackTrace);
            }
        }

        public void StartKafkaListner()
        {

            // topic에서 메세지 구독 요청
            consumer.Subscribe("quickstart-events");

            if (consumer != null)
            {
                Debug.Log("Kafka - Subscribed. " + consumer.Name);
            }
            else
            {
                Debug.Log("Kafka - Fail to Subscribed.");
            }

            // CancellationTokenSource 클래스
            // : CancellationToken(취소요청)생성, Cancel 요청을 CancellationToken들에게 보내는 역할
            // CancellationToken 구조체
            // : Cancel 상태를 모니터링하는 여러 Listner들에 의해 사용되는 구조체
            //   작업이나 메서드가 실행되는 동안 취소 요청이 발생하면 해당 작업이나 메서드가 취소될 수 있도록 하는 메커니즘 제공
            //   안정성이 높음
            // IsCancellationRequested : bool 타입 변수로, 작업 취소 여부를 알려줌
            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                Debug.Log("On loading");

                // 취소 요청이 들어올 때까지 반복 실행
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        Debug.Log("Wait for message...");

                        // 생성된 consumer 저장소에 메세지 받아올 명령 실행, 받아오는 기간 인자값에 대입
                        // ConsumeResult<Ignore, string> consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(5000));
                        var consumeResult = consumer.Consume(cts.Token);     // none : 취소 요청이 생기더라도 프로세스를 유지                           

                        if (consumeResult.IsPartitionEOF) // consumeResult 객체에서 현재 메세지가 partition의 끝에 도달했다면
                        {
                            Debug.Log(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }
                        // Debug.Log("Consume result: " + consumeResult.Message.Value);
                        // 성공적으로 consumer 메세지를 받아왔을 때 topic 이름, partition 번호, 오프셋 등과 json데이터값 표시
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
                        var message = consumeResult.Message.Value; // 받아온 메세지를 string 형식으로 대입
                                                                   //string _message = Encoding.UTF8.GetString(message);

                        // message queue에 받아온 string 값 추가
                        _queue.Enqueue(message);
                        // 선입선출

                        JObject json;

                        try
                        {
                            // 메세지열 json 형태로 파싱
                            json = JObject.Parse(message);
                            print(json);

                            string valuept = json["pt"]?.ToString(); // pt 파일 다운로드
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
                                Debug.Log(valuecsv);         // csv 파일 다운로드
                            }
                            else
                            {
                                Debug.Log("No csv data available.");
                            }
                        }
                        catch (JsonReaderException ex) // json 파싱 불가 시 오류 표시
                        {
                            //consumer.Commit();
                            Debug.LogError("Error parsing JSON: " + ex.Message);
                            Debug.LogError("JSON string: " + message);
                        }
                        consumer.Commit(consumeResult); // 수동 커밋
                    }
                    catch (ConsumeException e) // consume 불가 시 오류 표시
                    {

                        Debug.LogError("Kafka - Error occured: " + e.Error.Reason);
                    }
                }

            }
            catch (OperationCanceledException) // 프로세스 취소 요청 시 표시
            {
                Debug.Log("Kafka - Canceled..");
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }

    }

    public void Run_ManualAssign() // 수동 consume 메서드 
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
    public enum Mode // 비동기/수동 수신 모드 변경
    {
        subscribe,
        manual
    }
    Mode mode;

    // 유니티 내장 이벤트 함수 start()
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

        _handle = new threadHandle("211.193.31.69:9092", "my-group"); // 해당 스크립트 핸들러(생성자) 생성
        kafkaThread = new Thread(_handle.StartKafkaListner); // 핸들러를 통해 StartKafkaListner 스레드 생성


        kafkaThread.Start(); // 카프카 스레드 실행
        kafkaStarted = true;
        //StartKafkaListener(config);
    }
    private void ProcessKafkaMessage() // update 에서 지속적으로 실행
    {
        if (kafkaStarted)
        {
            string message;
            while (_handle._queue.TryDequeue(out message)) // 메세지큐에서 메세지를 사용시도 할 동안 실행
            {
                Debug.Log(message); // 사용된 메세지를 콘솔에 출력
                msg.text = message;
            }
        }
    }

    void StopKafkaThread() // 앱이 중지되거나 스크립트가 비활성화 될 때마다 실행
    {
        if (kafkaStarted)
        {
            Debug.Log("StopKafkaThread");

            kafkaThread.Abort(); // 카프카 스레드 중단
            kafkaThread.Join();
            kafkaStarted = false;
        }
    }
}

