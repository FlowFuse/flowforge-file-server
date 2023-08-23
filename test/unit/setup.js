const flowForgeFileServer = require('../../forge/fileServer')
const path = require('path')
const PACKAGE_ROOT = '../../'
const FileStorageRequire = file => require(path.join(PACKAGE_ROOT, file))
const http = require('http')

// setup authentication endpoint
function authServer (config = {}) {
    const host = config.host || 'localhost'
    const port = config.port || 3002
    const authConfig = config.authConfig || [
        { token: 'test-token', projectId: 'test-project' }
    ]
    const requestListener = function (req, res) {
        try {
            let authToken
            const urlParts = req.url.split('/')
            const projectId = urlParts.pop()
            const route = urlParts.join('/')
            switch (route) {
            case '/account/check/project':
                authToken = authConfig.find(auth => auth.projectId === projectId)
                if (req.headers.authorization === ('Bearer ' + authToken.token)) {
                    res.writeHead(200)
                    res.end('{ "file": 1000, "context": 1000 }')
                    return
                }
                throw new Error('Unknown request')
            default:
                res.writeHead(404)
                res.end(JSON.stringify({ error: 'Resource not found' }))
            }
        } catch (error) {
            res.writeHead(401)
            res.end(JSON.stringify({ error: 'unauthorised' }))
        }
    }

    const authServer = http.createServer(requestListener)
    authServer.listen(port, host, () => {
        // listening for requests on port 3002
    })
    return authServer
}

async function setupApp (config = {}) {
    config = config || {}
    const options = { }
    options.config = {
        logging: {
            level: 'silent'
        },
        FLOWFORGE_HOME: config.home || process.cwd(),
        FLOWFORGE_PROJECT_ID: config.projectId || 'test-project',
        FLOWFORGE_TEAM_ID: config.teamId || 'test-team',
        host: config.host || '0.0.0.0',
        port: config.port || 3001,
        base_url: config.base_url || 'http://localhost:3002',
        driver: {
            type: config.fileDriverType || 'memory',
            quota: config.contextQuota || 2000, // default context quota is 2KB
            options: config.fileDriverOptions || {
                root: 'var/root'
            }
        },
        context: {
            type: config.contextDriver || 'sequelize',
            quota: config.contextQuota || 2000, // default context quota is 2KB
            options: config.contextDriverOptions || { type: 'sqlite', storage: 'ff-context.db' }
        }
    }
    if (options.config.context.type === 'sequelize' && options.config.context.options.type === 'postgres') {
        try {
            const { Client } = require('pg')
            const client = new Client({
                host: options.config.context.options.host || 'localhost',
                port: options.config.context.options.port || 5432,
                user: options.config.context.options.username,
                password: options.config.context.options.password
            })
            await client.connect()
            try {
                await client.query(`DROP DATABASE "${options.config.context.options.database}"`)
            } catch (err) {
                // Don't mind if it doesn't exist
            }
            await client.query(`CREATE DATABASE "${options.config.context.options.database}"`)
            await client.end()
        } catch (err) {
            console.log(err.toString())
            process.exit(1)
        }
    }

    const server = await flowForgeFileServer(options)
    let stopping = false
    async function exitWhenStopped () {
        if (!stopping) {
            stopping = true
            await server.close()
        }
    }

    process.on('SIGINT', exitWhenStopped)
    process.on('SIGTERM', exitWhenStopped)
    process.on('SIGHUP', exitWhenStopped)
    process.on('SIGUSR2', exitWhenStopped) // for nodemon restart
    process.on('SIGBREAK', exitWhenStopped)
    process.on('message', function (m) { // for PM2 under window with --shutdown-with-message
        if (m === 'shutdown') { exitWhenStopped() }
    })

    // Start the server
    server.listen({ port: server.config.port, host: server.config.host }, function (err, address) {
        if (err) {
            console.error(err)
        }
    })
    return server
}

module.exports = {
    require: FileStorageRequire,
    resolve: file => path.resolve(path.join(__dirname, PACKAGE_ROOT, file)),
    setupApp,
    authServer
}
