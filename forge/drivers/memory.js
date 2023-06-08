const { join, sep: pathSeparator } = require('path')

const canary = 'ROOT_DIR_CANARY'
const storage = {}

function resolvePath (teamId, projectId, path) {
    let resolvedPath
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

module.exports = function (app) {
    return {
        get rootPath () {
            return ''
        },
        resolvePath,

        async ensureDir (teamId, projectId, path) {
            try {
                resolvePath(teamId, projectId, path)
                return true
            } catch (error) {
                return false
            }
        },

        async save (teamId, projectId, path, data) {
            storage[resolvePath(teamId, projectId, path)] = data
        },

        async append (teamId, projectId, path, data) {
            if (storage[resolvePath(teamId, projectId, path)]) {
                storage[resolvePath(teamId, projectId, path)] += data
            } else {
                storage[resolvePath(teamId, projectId, path)] = data
            }
        },

        async read (teamId, projectId, path) {
            return storage[resolvePath(teamId, projectId, path)]
        },

        async delete (teamId, projectId, path) {
            delete storage[resolvePath(teamId, projectId, path)]
        },

        async quota (teamId, projectId) {
            let used = 0
            Object.keys(storage).forEach(key => {
                if (key.startsWith(join(teamId, projectId))) {
                    used += storage[key].length
                }
            })
            return used
        }
    }
}
