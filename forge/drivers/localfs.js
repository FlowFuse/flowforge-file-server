const fs = require('fs')
const { join, isAbsolute, dirname, sep: pathSeparator, parse } = require('path')

const canary = 'ROOT_DIR_CANARY'

async function readDirSize (dir) {
    if (!fs.existsSync(dir)) {
        return 0
    }
    const files = fs.readdirSync(dir, { withFileTypes: true })
    const paths = files.map(async file => {
        const path = join(dir, file.name)
        if (file.isDirectory()) return await readDirSize(path)

        if (file.isFile()) {
            const { size } = fs.statSync(path)
            return size
        }
        return 0
    })

    return (await Promise.all(paths)).flat(Infinity).reduce((i, size) => i + size, 0)
}

function cleanError (error, path, op) {
    if (error.code === 'ENOENT') {
        const err = new Error(`ENOENT: no such file or directory, ${op || 'stat'} '${path}'`)
        err.code = error.code
        return err
    } else if (error.code === 'EPERM') {
        const err = new Error(`EPERM: operation not permitted, ${op || 'unlink'} '${path}'`)
        err.code = error.code
        return err
    }
    return error
}

module.exports = function (app) {
    let rootPath
    if (!isAbsolute(app.config.driver.options.root)) {
        rootPath = join(app.config.home, app.config.driver.options?.root)
    } else {
        rootPath = app.config.driver.options.root
    }

    if (!fs.existsSync(rootPath)) {
        fs.mkdirSync(rootPath, { recursive: true })
    }

    app.log.info(`FlowFuse File Storage localfs root Directory: ${rootPath}`)

    function resolvePath (teamId, projectId, path) {
        let resolvedPath
        if (isAbsolute(path)) {
            const pp = parse(path)
            const hasWinDrive = /[a-z|A-Z]:/.test(pp.root)
            if (hasWinDrive) {
                path = path.replace(':', '')
            }
        }
        const minPath = join(canary, teamId, projectId, path) // limit traversal to /teamid/projectId/**
        if (path.startsWith(pathSeparator)) {
            resolvedPath = join(canary, teamId, path)
        } else {
            resolvedPath = join(canary, teamId, projectId, path)
        }
        if (resolvedPath.startsWith(minPath)) {
            const array = resolvedPath.split(pathSeparator)
            array.shift()
            resolvedPath = array.join(pathSeparator)
            return resolvedPath
        } else {
            const err = new Error('Invalid Path')
            err.code = 'ENOTDIR'
            throw err
        }
    }

    return {
        get rootPath () {
            return rootPath
        },
        resolvePath,
        async ensureDir (teamId, projectId, path) {
            try {
                resolvePath(teamId, projectId, path)
                return true
            } catch (error) {
                throw cleanError(error, path, 'stat')
            }
        },
        async save (teamId, projectId, path, data) {
            const fullPath = join(rootPath, resolvePath(teamId, projectId, path))
            try {
                fs.mkdirSync(dirname(fullPath), {
                    recursive: true
                })
                fs.writeFileSync(fullPath, data)
            } catch (error) {
                throw cleanError(error, path, 'open')
            }
        },

        async append (teamId, projectId, path, data) {
            const fullPath = join(rootPath, resolvePath(teamId, projectId, path))
            if (fs.existsSync(fullPath)) {
                try {
                    fs.mkdirSync(dirname(fullPath), {
                        recursive: true
                    })
                    fs.appendFileSync(fullPath, data)
                } catch (error) {
                    throw cleanError(error, path, 'open')
                }
            } else {
                await this.save(teamId, projectId, path, data)
            }
        },

        async read (teamId, projectId, path) {
            const fullPath = join(rootPath, resolvePath(teamId, projectId, path))
            try {
                return fs.readFileSync(fullPath)
            } catch (error) {
                throw cleanError(error, path, 'open')
            }
        },

        async delete (teamId, projectId, path) {
            const fullPath = join(rootPath, resolvePath(teamId, projectId, path))
            try {
                fs.rmSync(fullPath)
            } catch (error) {
                throw cleanError(error, path, 'unlink')
            }
        },

        async quota (teamId, projectId) {
            return readDirSize(join(rootPath, teamId, projectId))
        }
    }
}
