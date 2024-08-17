using Confluent.Kafka;

namespace UseKafka.ProducerApi.services
{
    public class ProduceServices
    {
        private readonly ILogger<ProduceServices> _logger;

        public ProduceServices(ILogger<ProduceServices> logger)
        {
            _logger = logger;
        }

        public async Task ProduceAsync(CancellationToken cancellationToken)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                AllowAutoCreateTopics = true,
                Acks = Acks.All,

            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();
            try
            {
                var deliveryResult = await producer.ProduceAsync(topic: "test-topic",
                new Message<Null, string>
                {
                    Value = $"Hello, Kafka! {DateTime.UtcNow}"
                }, cancellationToken);

                _logger.LogInformation($"Delivered message to {deliveryResult.Value} , offset : {deliveryResult.Offset}");
            }
            catch (ProduceException<Null, string> e)
            {
                _logger.LogError($"Delivery failed: {e.Error.Reason}");
            }
            producer.Flush(cancellationToken);
        }
    }
}