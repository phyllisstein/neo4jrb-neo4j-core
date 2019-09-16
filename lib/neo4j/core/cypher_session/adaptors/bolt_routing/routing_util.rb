# frozen_string_literal: true

module Neo4j
  module Core
    module BoltRouting
      class RoutingUtil
        CALL_GET_SERVERS = 'CALL dbms.cluster.routing.getServers'
        PROCEDURE_NOT_FOUND_CODE = 'Neo.ClientError.Procedure.ProcedureNotFound'

        def initialize(routing_context)
          @routing_context = routing_context
        end

        def call_routing_procedure(session, router_address)
          call_available_routing_procedure(session)
        rescue => e
          raise Neo4j::Core::CypherSession::CypherError::ConnectionFailedError, "Server at #{ router_address } cannot perform routing. Make sure you are connecting to a causal cluster." if e.respond_to?(:code) && e.code == PROCEDURE_NOT_FOUND_CODE
        end

        def parse_servers(record, router_address)
          servers = record[:server]

          readers = servers.filter { |s| s[:role] == 'READ' }.pluck(:addresses)
          routers = servers.filter { |s| s[:role] == 'ROUTE' }.pluck(:addresses)
          writers = servers.filter { |s| s[:role] == 'WRITE' }.pluck(:addresses)

          {
            readers: readers,
            routers: routers,
            writers: writers,
          }
        rescue => e
          raise Neo4j::Core::CypherSession::CypherError::ConnectionFailedError, "Unable to parse servers entry from router #{ router_address } with record #{ record } (#{ e.message })."
        end

        def parse_ttl(record, router_address)
          record[:ttl] * 1000 + Time.now.to_i
        rescue => e
          raise Neo4j::Core::CypherSession::CypherError::ConnectionFailedError, "Unable to parse TTL entry from router #{ router_address } with record #{ record } (#{ e.message })."
        end

        private

        def call_available_routing_procedure(session)
          Neo4j::Transaction.run(session) do |tx|
            tx.query(CALL_GET_SERVERS)
          end
        end
      end
    end
  end
end
