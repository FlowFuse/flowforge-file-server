const { Sequelize, DataTypes, where, fn, col, Op } = require('sequelize')
const { getObjectProperty, getItemSize } = require('./quotaTools')
const util = require('@node-red/util').util
const path = require('path')

let sequelize, app

module.exports = {
    init: async function (_app) {
        app = _app
        const dbOptions = {
            dialect: app.config.context.options.type || 'sqlite',
            logging: !!app.config.context.options.logging
        }

        if (dbOptions.dialect === 'sqlite') {
            let filename = app.config.context.options.storage || 'context.db'
            if (filename !== ':memory:') {
                if (!path.isAbsolute(filename)) {
                    filename = path.join(app.config.home, 'var', filename)
                }
                dbOptions.storage = filename
                dbOptions.retry = {
                    match: [
                        /SQLITE_BUSY/
                    ],
                    name: 'query',
                    max: 10
                }
                dbOptions.pool = {
                    maxactive: 1,
                    max: 5,
                    min: 0,
                    idle: 2000
                }
            }
        } else if (dbOptions.dialect === 'postgres') {
            dbOptions.host = app.config.context.options.host || 'postgres'
            dbOptions.port = app.config.context.options.port || 5432
            dbOptions.username = app.config.context.options.username
            dbOptions.password = app.config.context.options.password
            dbOptions.database = app.config.context.options.database || 'ff-context'
        }

        sequelize = new Sequelize(dbOptions)

        app.log.info(`FlowForge File Server Sequelize Context connected to ${dbOptions.dialect} on ${dbOptions.host || dbOptions.storage}`)

        const Context = sequelize.define('Context', {
            project: { type: DataTypes.STRING, allowNull: false, unique: 'context-project-scope-unique' },
            scope: { type: DataTypes.STRING, allowNull: false, unique: 'context-project-scope-unique' },
            values: { type: DataTypes.JSON, allowNull: false }
        })
        await sequelize.sync()
        this.Context = Context
    },
    /**
     * Set the context data for a given scope
     * @param {string} projectId - The project id
     * @param {string} scope - The context scope to write to
     * @param {[{key:string, value:any}]} input - The context data to write
     * @param {boolean} [overwrite=false] - If true, any context data will be overwritten (i.e. for a cache dump). If false, the context data will be merged with the existing data.
     */
    set: async function (projectId, scope, input, overwrite = false) {
        const { path } = parseScope(scope)
        await sequelize.transaction({
            type: Sequelize.Transaction.TYPES.IMMEDIATE
        },
        async (t) => {
            // get the existing row of context data from the database (if any)
            let existingRow = await this.Context.findOne({
                where: {
                    project: projectId,
                    scope: path
                },
                lock: t.LOCK.UPDATE,
                transaction: t
            })
            const quotaLimit = app.config?.context?.quota || 0
            // if quota is set, check if we are over quota or will be after this update
            if (quotaLimit > 0) {
                // Difficulties implementing this correctly
                // - The final size of data can only be determined after the data is stored.
                //   This is due to the fact that some keys may be deleted and some may be added
                //   and the size of the data is not the same as the size of the keys.
                // This implementation is not ideal, but it is a good approximation and will
                //   prevent the possibility of runaway storage usage.
                let changeSize = 0
                let hasValues = false
                // if we are overwriting, then we need to remove the existing size to get the final size
                if (existingRow) {
                    if (overwrite) {
                        changeSize -= getItemSize(existingRow.values || '')
                    } else {
                        hasValues = existingRow?.values && Object.keys(existingRow.values).length > 0
                    }
                }
                // calculate the change in size
                for (const element of input) {
                    const currentItem = hasValues ? getObjectProperty(existingRow.values, element.key) : undefined
                    if (currentItem === undefined && element.value !== undefined) {
                        // this is an addition
                        changeSize += getItemSize(element.value)
                    } else if (currentItem !== undefined && element.value === undefined) {
                        // this is an deletion
                        changeSize -= getItemSize(currentItem)
                    } else {
                        // this is an update
                        changeSize -= getItemSize(currentItem)
                        changeSize += getItemSize(element.value)
                    }
                }
                // only calculate the current size if we are going to need it
                if (changeSize >= 0) {
                    const currentSize = await this.quota(projectId)
                    if (currentSize + changeSize > quotaLimit) {
                        const err = new Error('Over Quota')
                        err.code = 'over_quota'
                        err.error = err.message
                        err.limit = quotaLimit
                        throw err
                    }
                }
            }

            // if we are overwriting, then we need to reset the values in the existing row (if any)
            if (existingRow && overwrite) {
                existingRow.values = {} // reset the values since this is a mem cache -> DB dump
            }

            // if there is no input, then we are probably deleting the row
            if (input?.length > 0) {
                if (!existingRow) {
                    existingRow = await this.Context.create({
                        project: projectId,
                        scope: path,
                        values: {}
                    },
                    {
                        transaction: t
                    })
                }
                for (const i in input) {
                    const path = input[i].key
                    const value = input[i].value
                    util.setMessageProperty(existingRow.values, path, value)
                }
            }
            if (existingRow) {
                if (existingRow.values && Object.keys(existingRow.values).length === 0) {
                    await existingRow.destroy({ transaction: t })
                } else {
                    existingRow.changed('values', true)
                    await existingRow.save({ transaction: t })
                }
            }
        })
    },
    /**
     * Get the context data for a given scope
     * @param {string} projectId - The project id
     * @param {string} scope - The context scope to read from
     * @param {[string]} keys - The context keys to read
     * @returns {[{key:string, value?:any}]} - The context data
    */
    get: async function (projectId, scope, keys) {
        const { path } = parseScope(scope)
        const row = await this.Context.findOne({
            attributes: ['values'],
            where: {
                project: projectId,
                scope: path
            }
        })
        const values = []
        if (row) {
            const data = row.values
            keys.forEach(key => {
                try {
                    const value = util.getObjectProperty(data, key)
                    values.push({
                        key,
                        value
                    })
                } catch (err) {
                    if (err.code === 'INVALID_EXPR') {
                        throw err
                    }
                    values.push({
                        key
                    })
                }
            })
        }
        return values
    },
    /**
     * Get all context values for a project
     * @param {string} projectId The project id
     * @param {object} pagination The pagination settings
     * @param {number} pagination.limit The maximum number of rows to return
     * @param {string} pagination.cursor The cursor to start from
     * @returns {[{scope: string, values: object}]}
     */
    getAll: async function (projectId, pagination = {}) {
        const where = { project: projectId }
        const limit = parseInt(pagination.limit) || 1000
        const rows = await this.Context.findAll({
            attributes: ['id', 'scope', 'values'],
            where: buildPaginationSearchClause(pagination, where),
            order: [['id', 'ASC']],
            limit
        })
        const count = await this.Context.count({ where })
        const data = rows?.map(row => {
            const dataRow = { scope: row.dataValues.scope, values: row.dataValues.values }
            const { scope } = parseScope(dataRow.scope)
            dataRow.scope = scope
            return dataRow
        })
        return {
            meta: {
                next_cursor: rows.length === limit ? rows[rows.length - 1].id : undefined
            },
            count,
            data
        }
    },
    keys: async function (projectId, scope) {
        const { path } = parseScope(scope)
        const row = await this.Context.findOne({
            attributes: ['values'],
            where: {
                project: projectId,
                scope: path
            }
        })
        if (row) {
            return Object.keys(row.values)
        } else {
            return []
        }
    },
    delete: async function (projectId, scope) {
        const { path } = parseScope(scope)
        const existing = await this.Context.findOne({
            where: {
                project: projectId,
                scope: path
            }
        })
        if (existing) {
            await existing.destroy()
        }
    },
    clean: async function (projectId, activeIds) {
        activeIds = activeIds || []
        const scopesResults = await this.Context.findAll({
            where: {
                project: projectId
            }
        })
        const scopes = scopesResults.map(s => s.scope)
        if (scopes.includes('global')) {
            scopes.splice(scopes.indexOf('global'), 1)
        }
        if (scopes.length === 0) {
            return
        }
        const keepFlows = []
        const keepNodes = []
        for (const id of activeIds) {
            for (const scope of scopes) {
                if (scope.startsWith(`${id}.flow`)) {
                    keepFlows.push(scope)
                } else if (scope.endsWith(`.nodes.${id}`)) {
                    keepNodes.push(scope)
                }
            }
        }

        for (const scope of scopes) {
            if (keepFlows.includes(scope) || keepNodes.includes(scope)) {
                continue
            } else {
                const r = await this.Context.findOne({
                    where: {
                        project: projectId,
                        scope
                    }
                })
                r && await r.destroy()
            }
        }
    },
    quota: async function (projectId) {
        const scopesResults = await this.Context.findAll({
            where: {
                project: projectId
            }
        })
        let size = 0
        scopesResults.forEach(scope => {
            const strValues = JSON.stringify(scope.values)
            size += strValues.length
        })
        return size
    }
}

/**
 * Parse a scope string into its parts
 * @param {String} scope the scope to parse, passed in from node-red or the database
 */
function parseScope (scope) {
    let type, path
    let flow = null
    let node = null
    if (scope === 'global') {
        type = 'global'
        path = 'global'
    } else if (scope.indexOf('.nodes.') > -1) {
        // node context (db scope format  <flowId>.nodes.<nodeId>)
        const parts = scope.split('.nodes.')
        type = 'node'
        flow = '' + parts[0]
        node = '' + parts[1]
        scope = `${node}:${flow}`
        path = scope
    } else if (scope.endsWith('.flow')) {
        // flow context (db scope format  <flowId>.flow)
        path = scope
        flow = scope.replace('.flow', '')
        scope = flow
        type = 'flow'
    } else if (scope.indexOf(':') > -1) {
        // node context (node-red scope format  <nodeId>:<flowId>)
        const parts = scope.split(':')
        type = 'node'
        flow = '' + parts[1]
        node = '' + parts[0]
        path = `${flow}.nodes.${node}`
    } else {
        // flow context
        type = 'flow'
        path = `${scope}.flow`
    }
    return { type, scope, path, flow, node }
}

/**
 * Generate a properly formed where-object for sequelize findAll, that applies
 * the required pagination, search and filter logic
 *
 * @param {Object} params the pagination options - cursor, query, limit
 * @param {Object} whereClause any pre-existing where-query clauses to include
 * @param {Array<String>} columns an array of column names to search.
 * @returns a `where` object that can be passed to sequelize query
 */
function buildPaginationSearchClause (params, whereClause = {}, columns = [], filterMap = {}) {
    whereClause = { ...whereClause }
    if (params.cursor) {
        whereClause.id = { [Op.gt]: params.cursor }
    }
    whereClause = {
        [Op.and]: [
            whereClause
        ]
    }

    for (const [key, value] of Object.entries(filterMap)) {
        if (Object.hasOwn(params, key)) {
            // A filter has been provided for key
            let clauseContainer = whereClause[Op.and]
            let param = params[key]
            if (Array.isArray(param)) {
                if (param.length > 1) {
                    clauseContainer = []
                    whereClause[Op.and].push({ [Op.or]: clauseContainer })
                }
            } else {
                param = [param]
            }
            param.forEach(p => {
                clauseContainer.push(where(fn('lower', col(value)), p.toLowerCase()))
            })
        }
    }
    if (params.query && columns.length) {
        const searchTerm = `%${params.query.toLowerCase()}%`
        const searchClauses = columns.map(colName => {
            return where(fn('lower', col(colName)), { [Op.like]: searchTerm })
        })
        const query = {
            [Op.or]: searchClauses
        }
        whereClause[Op.and].push(query)
    }
    return whereClause
}
