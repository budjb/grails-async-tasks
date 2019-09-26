package com.budjb.asynchronoustasks

import grails.gorm.transactions.Transactional
import grails.validation.ValidationException

/**
 * Service for {@link AsynchronousTaskDomain}.
 */
@Transactional
class AsynchronousTaskService {
    AsynchronousTaskDomain makeAsynchronousTaskDomain(String taskName, String description) {
        AsynchronousTaskDomain domain = new AsynchronousTaskDomain()

        domain.name = taskName
        domain.description = description

        return domain
    }

    AsynchronousTaskDomain makeAndSaveAsynchronousTaskDomain(String taskName, String description) throws ValidationException {
        return save(makeAsynchronousTaskDomain(taskName, description))
    }

    AsynchronousTaskDomain save(AsynchronousTaskDomain domain) throws ValidationException {
        if (!domain.validate()) {
            throw new ValidationException("can not create a domain instance for task ${getTaskName()} due to validation errors", domain.errors)
        }

        domain.save(flush: true, failOnError: true)
    }

    AsynchronousTaskDomain get(Serializable id) {
        return AsynchronousTaskDomain.get(id)
    }

    AsynchronousTaskDomain read(Serializable id) {
        return AsynchronousTaskDomain.read(id)
    }
}
