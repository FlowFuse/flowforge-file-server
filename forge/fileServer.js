const fastify = require('fastify')
const auth = require('./auth')
const config = require('./config')
const driver = require('./driver')
const routes = require('./routes')
const helmet = require('@fastify/helmet')

module.exports = async (options = {}) => {
    const runtimeConfig = config.init(options)
    const loggerConfig = {
        formatters: {
            level: (label) => {
                return { level: label.toUpperCase() }
            },
            bindings: (bindings) => {
                return { }
            }
        },
        timestamp: require('pino').stdTimeFunctions.isoTime,
        level: runtimeConfig.logging.level,
        serializers: {
            res (reply) {
                return {
                    statusCode: reply.statusCode,
                    request: {
                        url: reply.request?.raw?.url,
                        method: reply.request?.method,
                        remoteAddress: reply.request?.ip,
                        remotePort: reply.request?.socket.remotePort
                    }
                }
            }
        }
    }
    if (runtimeConfig.logging.pretty !== false) {
        loggerConfig.transport = {
            target: 'pino-pretty',
            options: {
                translateTime: "UTC:yyyy-mm-dd'T'HH:MM:ss.l'Z'",
                ignore: 'pid,hostname',
                singleLine: true
            }
        }
    }

    const server = fastify({
        bodyLimit: 10 * 1024 * 1024, // Limit set to 10MB,
        maxParamLength: 500,
        trustProxy: true,
        logger: loggerConfig
    })

    if (runtimeConfig.telemetry.backend?.prometheus?.enabled) {
        const metricsPlugin = require('fastify-metrics')
        await server.register(metricsPlugin, { endpoint: '/metrics' })
    }

    server.addHook('onError', async (request, reply, error) => {
        // Useful for debugging when a route goes wrong
        // console.log(error.stack)
    })

    try {
        // Config
        await server.register(config.attach, options)

        // // Setup DB
        // await server.register(db, {})

        // // Setup settings
        // await server.register(settings, {})

        // Authentication Handler
        await server.register(auth, {})

        // HTTP Server setup
        await server.register(helmet, {
            global: true,
            hidePoweredBy: true,
            hsts: false,
            frameguard: {
                action: 'deny'
            }
        })

        // Driver
        await server.register(driver, {})

        // Routes
        await server.register(routes, { logLevel: server.config.logging.http })

        server.ready()

        return server
    } catch (err) {
        server.log.error(`Failed to start: ${err.toString()}`)
        throw err
    }
}
