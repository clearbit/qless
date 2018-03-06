# Encoding: utf-8

# Qless requires
require 'qless'
require 'qless/worker/base'

# Note that this uses a single redis connection rather than a pool which may
# cause contention depending on your workload.

module Qless
  module Workers
    # A worker that keeps popping off jobs and processing them
    class ThreadedWorker < BaseWorker
      def initialize(reserver, options = {})
        super(reserver, options)

        @thread_count = options[:threads] || 5

        remove_instance_variable(:@current_job_mutex)
        remove_instance_variable(:@current_job)
      end

      def run
        log(:info, "Starting #{reserver.description} in #{Process.pid}")
        procline "Starting #{reserver.description}"
        register_signal_handlers

        reserver.prep_for_work!

        procline "Running #{reserver.description} with #{@thread_count} threads"

        threads = []

        @thread_count.times do
          threads << Thread.new do
            listen_for_lost_lock do
              jobs.each do |job|
                # Run the job we're working on
                log(:debug, "Starting job #{job.klass_name} (#{job.jid} from #{job.queue_name})")
                perform(job)
                log(:debug, "Finished job #{job.klass_name} (#{job.jid} from #{job.queue_name})")

                # So long as we're paused, we should wait
                while paused
                  log(:debug, 'Paused...')
                  sleep interval
                end
              end
            end
          end
        end

        threads.each(&:join)
      end

      def with_current_job
        yield Thread.current[:current_job]
      end

      def current_job=(job)
        Thread.current[:current_job] = job
      end
    end
  end
end
