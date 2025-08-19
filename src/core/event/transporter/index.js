const { EventRabbitMQTransporter } = require('./rabbitmq-transporter.event.core');
const { EventDirectTransporter } = require('./direct-transporter.event.core');

module.exports = {
  EventRabbitMQTransporter: EventRabbitMQTransporter,
  EventDirectTransporter: EventDirectTransporter,
};