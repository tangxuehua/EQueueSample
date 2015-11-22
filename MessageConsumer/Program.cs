using System;
using System.Net;
using System.Threading;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Socketing;
using EQueue.Clients.Consumers;
using EQueue.Configurations;
using EQueue.Protocols;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace MessageConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
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
                .RegisterEQueueComponents();

            var setting = new ConsumerSetting
            {
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset,
                MessageHandleMode = MessageHandleMode.Sequential,
                BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5001),
                BrokerAdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002)
            };
            new Consumer("group1", setting)
                .Subscribe("topic1")
                .SetMessageHandler(new MessageHandler())
                .Start();
        }

        class MessageHandler : IMessageHandler
        {
            private long _previusHandledCount;
            private long _handledCount;
            private long _calculateCount = 0;
            private IScheduleService _scheduleService;
            private ILogger _logger;

            public MessageHandler()
            {
                _scheduleService = ObjectContainer.Resolve<IScheduleService>();
                _scheduleService.StartTask("PrintThroughput", PrintThroughput, 1000, 1000);
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                Interlocked.Increment(ref _handledCount);
                Thread.Sleep(20);
                context.OnMessageHandled(message);
            }

            private void PrintThroughput()
            {
                var totalHandledCount = _handledCount;
                var throughput = totalHandledCount - _previusHandledCount;
                _previusHandledCount = totalHandledCount;
                if (throughput > 0)
                {
                    _calculateCount++;
                }

                var average = 0L;
                if (_calculateCount > 0)
                {
                    average = totalHandledCount / _calculateCount;
                }

                _logger.InfoFormat("totalReceived: {0}, throughput: {1}/s, average: {2}", totalHandledCount, throughput, average);
            }
        }
    }
}
