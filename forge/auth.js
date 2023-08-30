const fp = require('fastify-plugin')
const got = require('got')

const authCache = {}
const ttl = 90 * 1000

module.exports = fp(async function (app, opts, done) {
    const client = got.extend({
        prefixUrl: `${app.config.base_url}/account/check/project`,
        headers: {
            'user-agent': 'FlowForge Storage Server',
            'ff-quota': true
        },
        timeout: {
            request: 500
        }
    })

    async function checkToken (projectId, token) {
        const project = await client.get(projectId, {
            headers: {
                authorization: `Bearer ${token}`
            }
        })
        return project.body
    }

    async function checkAuth (request, reply) {
        try {
            const token = getAuthToken(request)
            const cacheOk = checkCache(token, request.params.projectId)
            if (!cacheOk) {
                const tokenResponse = await checkToken(request.params.projectId, token)
                if (!tokenResponse) {
                    throw new Error('Invalid token')
                }
                request.quota = tokenResponse.quota
                // update cache
                authCache[token] = {
                    ttl: Date.now(),
                    projectId: request.params.projectId,
                    quota: tokenResponse.quota
                }
            } else {
                request.quota = authCache[token].quota
            }
        } catch (error) {
            // always send 401 for security reasons
            reply.code(401).send({ code: 'unauthorized', error: 'unauthorized' })
        }
    }

    /**
     * Extract the token from the request.
     * @throws {Error} if the token is not found or the authorization header is invalid
     * @param {Object} request The request object
     * @returns {string} The token
     */
    function getAuthToken (request) {
        if (!request?.headers?.authorization) {
            throw new Error('Missing authorization header')
        }
        const parts = request.headers.authorization.split(' ')
        if (parts.length !== 2) {
            throw new Error('Invalid authorization header')
        }
        if (parts[0] !== 'Bearer') {
            throw new Error('Invalid authorization header')
        }
        if (!parts[1]) {
            throw new Error('Invalid authorization header')
        }
        return parts[1]
    }

    /**
     * Check if the token is in the cache and if it is still valid
     * @param {string} token The token to check
     * @param {string} projectId The projectId to check
     * @returns {boolean} true if the token is valid for the projectId
     */
    function checkCache (token, projectId) {
        const cacheExists = authCache[token] && authCache[token].projectId && authCache[token].ttl
        let projectMatch = false
        let ttlOk = false
        if (cacheExists) {
            const cacheEntry = authCache[token]
            // ensure projectId matches the cached projectId
            projectMatch = cacheEntry.projectId === projectId
            // check cache ttl
            ttlOk = (Date.now() - cacheEntry.ttl) < ttl
        }
        const result = cacheExists && projectMatch && ttlOk
        // delete only if expired
        if (!ttlOk && projectMatch) {
            delete authCache[token]
        }
        return result
    }

    app.decorate('checkAuth', checkAuth)
    done()
})
