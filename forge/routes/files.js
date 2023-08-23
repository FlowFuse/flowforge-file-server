/**
 * File Storage API
 *
 * @namespace files
 * @memberof forge.fileserver
 */

/** @typedef {import('fastify')} Fastify */
/** @typedef {import('fastify').FastifyReply} FastifyReply */
/** @typedef {import('fastify').FastifyRequest} FastifyRequest */

module.exports = async function (app, opts, done) {
    app.addContentTypeParser('application/octet-stream', { parseAs: 'buffer' }, function (request, payload, done) {
        done(null, payload)
    })

    /**
     * Create/Update a File
     *
     * @name /v1/files/:teamId/:projectId/*
     * @static
     * @memberof forge.fileserver.files
     */
    app.post('/:teamId/:projectId/*', {

    }, async (request, reply) => {
        const path = request.params['*']
        let quota = -1
        if (request.quota?.file) {
            quota = request.quota.file
        } else if (app.config.driver.quota) {
            quota = app.config.driver.quota
        }
        let currentSize = -1
        if (quota !== -1) {
            currentSize = await request.vfs.quota()
        }
        try {
            if (request.headers.ff_mode === 'append') {
                if (currentSize !== -1) {
                    const newSize = currentSize + request.body.length
                    if (newSize < quota) {
                        await request.vfs.append(path, request.body)
                    } else {
                        reply.code(413).send({ code: 'over_quota', error: 'Over Quota', limit: quota })
                        return
                    }
                } else {
                    await request.vfs.append(path, request.body)
                }
            } else if (request.headers.ff_mode === 'ensureDir') {
                await request.vfs.ensureDir(path, request.body)
            } else {
                // should really check if file exists first and compute the delta
                if (currentSize !== -1) {
                    const newSize = currentSize + request.body.length
                    if (newSize < quota) {
                        await request.vfs.save(path, request.body)
                    } else {
                        reply.code(413).send({ code: 'over_quota', error: 'Over Quota', limit: quota })
                        return
                    }
                } else {
                    await request.vfs.save(path, request.body)
                }
            }
            reply.code(200).send()
        } catch (err) {
            console.log(err)
            reply.code(400).send(err)
        }
    })

    // /**
    //  * Stat a file
    //  *
    //  * @name /v1/files/:teamId/:projectId/*
    //  * @static
    //  * @memberof forge.fileserver.files
    //  */
    //  app.head('/:teamId/:projectId/*', async (request, reply) => {

    //  })

    /**
     * Retrieve a file
     *
     * @name /v1/files/:teamId/:projectId/*
     * @static
     * @memberof forge.fileserver.files
     */
    app.get('/:teamId/:projectId/*', async (/** @type {FastifyRequest} */ request, /** @type {FastifyReply} */ reply) => {
        const path = request.params['*']
        try {
            const file = await request.vfs.read(path)
            if (file) {
                reply.send(file)
            } else {
                reply.code(404).send()
            }
        } catch (err) {
            if (err.code === 'ENOENT') {
                reply.code(404).send(err)
            } else if (err.code === 'ENOTDIR') {
                reply.code(400).send(err)
            } else {
                reply.code(500).send(err)
            }
        }
    })

    /**
     * Delete a file
     *
     * @name /v1/files/:teamId/:projectId/*
     * @static
     * @memberof forge.fileserver.files
     */
    app.delete('/:teamId/:projectId/*', async (request, reply) => {
        const path = request.params['*']
        try {
            await request.vfs.delete(path)
            reply.code(200).send()
        } catch (err) {
            reply.code(err.statusCode || 400).send(err)
        }
    })

    done()
}
