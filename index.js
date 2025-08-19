const amqplib = require('amqplib');

const { EventManagerFactory } = require('./src/core/event/manager/event-manager.core');
const EventTransporter = require('./src/core/event/transporter');

module.exports = { EventManager: EventManagerFactory(), EventManagerFactory, EventTransporter };