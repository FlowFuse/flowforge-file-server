
const fs = require('fs')
const fp = require('fastify-plugin')
const path = require('path')
const xbytes = require('xbytes')
const YAML = require('yaml')

module.exports = fp(async function (app, opts, next) {
    if (!opts.config && process.env.FF_FS_TEST_CONFIG) {
        opts.config = YAML.parse(process.env.FF_FS_TEST_CONFIG)
    }
    const testMode = !!opts.config
    if (testMode) {
        // A custom config has been passed in. This means we're running
        // programmatically rather than manually. At this stage, that
        // means its our test framework.
        process.env.NODE_ENV = 'development'
        process.env.FLOWFORGE_HOME = opts.config.FLOWFORGE_HOME || process.cwd()
    } else if (!process.env.FLOWFORGE_HOME) {
        if (process.env.NODE_ENV === 'development') {
            app.log.info('Development mode')
            process.env.FLOWFORGE_HOME = path.resolve(__dirname, '..')
        } else {
            if (fs.existsSync('/opt/flowforge-file-storage')) {
                process.env.FLOWFORGE_HOME = '/opt/flowforge-file-storage'
            } else {
                process.env.FLOWFORGE_HOME = process.cwd()
            }
        }
    }

    let ffVersion
    if (process.env.npm_package_version) {
        ffVersion = process.env.npm_package_version
        // npm start
    } else {
        // everything else
        const { version } = require(path.join(module.parent.path, '..', 'package.json'))
        ffVersion = version
    }
    try {
        fs.statSync(path.join(__dirname, '..', '..', '.git'))
        ffVersion += '-git'
    } catch (err) {
        // No git directory
    }

    app.log.info(`FlowForge File Storage v${ffVersion}`)
    app.log.info(`FlowForge File Storage running with NodeJS ${process.version}`)
    app.log.info(`FlowForge File Storage HOME Directory: ${process.env.FLOWFORGE_HOME}`)

    let config

    if (testMode) {
        app.log.info('FlowForge File Storage is running in test mode')
        config = { ...opts.config }
    } else {
        let configFile = path.join(process.env.FLOWFORGE_HOME, '/etc/flowforge-storage.yml')
        if (fs.existsSync(path.join(process.env.FLOWFORGE_HOME, '/etc/flowforge-storage.local.yml'))) {
            configFile = path.join(process.env.FLOWFORGE_HOME, '/etc/flowforge-storage.local.yml')
        }
        try {
            app.log.info(`FlowForge File Storage Config File: ${configFile}`)
            const configFileContent = fs.readFileSync(configFile, 'utf-8')
            config = YAML.parse(configFileContent)
        } catch (err) {
            app.log.error(`Failed to read config file ${configFile}: ${err.message}`)
        }
    }

    config.version = ffVersion
    config.home = process.env.FLOWFORGE_HOME
    config.port = process.env.PORT || config.port || 3001
    config.host = config.host || 'localhost'

    if (!config.driver) {
        config.driver = { type: 'localfs' }
    }

    if (!config.logging) {
        config.logging = {
            level: 'info',
            http: 'warn'
        }
    } else {
        if (!config.logging.http) {
            config.logging.http = 'warn'
        }
    }

    if (config.driver?.quota) {
        const parsed = xbytes.parse(config.driver.quota)
        config.driver.quota = parsed.bytes
    }

    if (config.context?.quota) {
        const parsed = xbytes.parse(config.context.quota)
        config.context.quota = parsed.bytes
    }
    Object.freeze(config)
    app.decorate('config', config)

    next()
})
