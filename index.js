#!/usr/bin/env node
'use strict'

const semver = require('semver')
const flowForgeFileServer = require('./forge/fileServer')
const YAML = require('yaml')

const app = (async function () {
    if (!semver.satisfies(process.version, '>=16.0.0')) {
        console.error(`FlowForge File Server requires at least NodeJS v16, ${process.version} found`)
        process.exit(1)
    }

    try {
        const options = {}
        // The tests for `nr-persistent-context` run a local copy of the file-server
        // and pass in the full config via this env var
        if (process.env.FF_FS_TEST_CONFIG) {
            try {
                options.config = YAML.parse(process.env.FF_FS_TEST_CONFIG)
            } catch (err) {
                console.error('Failed to parse FF_FS_TEST_CONFIG:', err)
                process.exitCode = 1
                return
            }
        }

        const server = await flowForgeFileServer(options)
        let stopping = false
        async function exitWhenStopped () {
            if (!stopping) {
                stopping = true
                server.log.info('Stopping FlowForge File Server')
                await server.close()
                server.log.info('FlowForge File Server stopped')
                process.exit(0)
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
                process.exit(1)
            }
        })
        return server
    } catch (err) {
        console.error(err)
        process.exitCode = 1
    }
})()

module.exports = app
