package com.budjb.asynchronoustasks

import grails.gorm.transactions.Rollback
import grails.gorm.transactions.Transactional
import grails.testing.mixin.integration.Integration
import spock.lang.Specification

@Integration
@Rollback
class SimpleFunctionalitySpec extends Specification {
    AsynchronousTaskService asynchronousTaskService

    def 'Ensure a simple persistent task functions correctly'() {
        setup:
        PersistentAsynchronousTask task = new PersistentAsynchronousTask(asynchronousTaskService, 'simpleTask', 'A basic task for testing.') {
            @Override
            protected void process() {
                save {
                    complete(null, 'simple task completed')
                }
            }
        }

        expect:
        task.taskId > 0
        task.state == AsynchronousTaskState.NOT_RUNNING
        task.results == null

        when:
        task.run()

        then:
        task.progress == 100
        task.state == AsynchronousTaskState.COMPLETED
        task.results == 'simple task completed'
    }
}
