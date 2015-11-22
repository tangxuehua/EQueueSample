using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Socketing;
using ECommon.Utilities;
using EQueue.Clients.Producers;
using EQueue.Configurations;
using EQueue.Protocols;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace MessageProducer
{
    class Program
    {
        static long _previousSentCount = 0;
        static long _sentCount = 0;
        static long _calculateCount = 0;
        static IScheduleService _scheduleService;
        static ILogger _logger;
        static ILogger _sendResultLogger;

        static void Main(string[] args)
        {
            InitializeEQueue();
            Task.Factory.StartNew(SendMessages);
            StartPrintThroughputTask();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterEQueueComponents()
                .SetDefault<IQueueSelector, QueueAverageSelector>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);
            _sendResultLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("SendResultLogger");
        }
        static void SendMessages()
        {
            var setting = new ProducerSetting
            {
                BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000),
                BrokerAdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002)
            };
            var producer = new Producer(setting).Start();
            var index = 0L;

            while (true)
            {
                var payload = Encoding.UTF8.GetBytes(index.ToString());
                var message = new Message("topic1", 100, payload);

                SendMessageAsync(producer, message, index.ToString());

                index++;
                Thread.Sleep(10);
            }
        }
        static Task SendMessageAsync(Producer producer, Message message, string routingKey)
        {
            return producer.SendAsync(message, routingKey).ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    _logger.ErrorFormat("Send message has exception, errorMessage: {0}", t.Exception.GetBaseException().Message);
                    return;
                }
                if (t.Result == null)
                {
                    _logger.Error("Send message timeout.");
                    return;
                }
                if (t.Result.SendStatus != SendStatus.Success)
                {
                    _logger.ErrorFormat("Send message failed, errorMessage: {0}", t.Result.ErrorMessage);
                    return;
                }

                _sendResultLogger.InfoFormat("Message send success, routingKey: {0}, sendResult: {1}", routingKey, t.Result);
                Interlocked.Increment(ref _sentCount);
            });
        }
        static void StartPrintThroughputTask()
        {
            _scheduleService.StartTask("PrintThroughput", () =>
            {
                var totalSentCount = _sentCount;
                var throughput = totalSentCount - _previousSentCount;
                _previousSentCount = totalSentCount;
                if (throughput > 0)
                {
                    _calculateCount++;
                }

                var average = 0L;
                if (_calculateCount > 0)
                {
                    average = totalSentCount / _calculateCount;
                }
                _logger.InfoFormat("Send message, totalSent: {0}, throughput: {1}/s, average: {2}", totalSentCount, throughput, average);
            }, 1000, 1000);
        }
    }
}
