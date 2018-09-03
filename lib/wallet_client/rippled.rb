# encoding: UTF-8
# frozen_string_literal: true

require 'securerandom'

module WalletClient
  class Rippled < Base

    def initialize(*)
      super
      @json_rpc_call_id  = 0
      @json_rpc_endpoint = URI.parse(wallet.uri)
    end

    def latest_block_number
      Rails.cache.fetch :latest_ripple_ledger, expires_in: 5.seconds do
        response = json_rpc(:ledger, [{ "ledger_index": 'validated' }])
        response.dig('result', 'ledger_index').to_i
      end
    end

    def create_address!(options = {})
      {
        address: "#{wallet.address}?dt=#{generate_destination_tag}",
        secret: wallet.settings['secret']
      }
    end

    def create_raw_address!(options = {})
      secret = options.fetch(:secret) { Passgen.generate(length: 64, symbols: true) }
      json_rpc(:wallet_propose, { passphrase: secret }).fetch('result')
                                                       .yield_self do |result|
        result.slice('key_type', 'master_seed', 'master_seed_hex',
                      'master_key', 'public_key', 'public_key_hex')
              .merge(address: normalize_address(result.fetch('account_id')), secret: secret)
              .symbolize_keys
      end
    end

    def normalize_address(address)
      address.gsub(/\?dt=\d*\Z/, '')
    end

    def destination_tag_from(address)
      address =~ /\?dt=(\d*)\Z/
      $1.to_i
    end

    def create_withdrawal!(issuer, recipient, amount, _options = {})
      tx_blob = sign_transaction(issuer, recipient, amount)
      json_rpc(:submit, tx_blob).fetch('result').yield_self do |result|
        result_message = result.fetch('engine_result_message')

        if result['engine_result'].to_s == 'tesSUCCESS' && result['status'].to_s == 'success'
          normalize_txid(result.fetch('tx_json').fetch('hash'))
        else
          raise Error, "XRP withdrawal from #{issuer.fetch(:address)} to #{recipient.fetch(:address)} failed: #{result_message}."
        end
      end
    end

    def sign_transaction(issuer, recipient, amount)
      account_address = normalize_address(issuer.fetch(:address))
      destination_address = normalize_address(recipient.fetch(:address))
      destination_tag = destination_tag_from(recipient.fetch(:address))

      params = {
        secret: issuer.fetch(:secret),
        fee_mult_max: 1000,
        tx_json: {
          Account:         account_address,
          Amount:          convert_to_base_unit!(amount).to_s,
          Destination:     destination_address,
          DestinationTag:  destination_tag,
          TransactionType: 'Payment',
          LastLedgerSequence: latest_block_number + 4
        }
      }

      json_rpc(:sign, params).fetch('result').yield_self do |result|
        if result['status'].to_s == 'success'
          { tx_blob: result['tx_blob'] }
        else
          raise Error, "XRP sign transaction from #{account_address} to #{destination_address} failed: #{result_message}."
        end
      end
    end

    protected

    def connection
      Faraday.new(@json_rpc_endpoint).tap do |connection|
        unless @json_rpc_endpoint.user.blank?
          connection.basic_auth(@json_rpc_endpoint.user, @json_rpc_endpoint.password)
        end
      end
    end
    memoize :connection

    def json_rpc(method, params = [])
      body = {
        jsonrpc: '1.0',
        id: @json_rpc_call_id += 1,
        method: method,
        params: [params].flatten
      }.to_json

      headers = {
        'Accept' => 'application/json',
        'Content-Type' => 'application/json'
      }

      response = connection.post('/', body, headers).yield_self do |response|
        response.assert_success!.yield_self do |response|
          JSON.parse(response.body).tap do |response|
            response.dig('result', 'error').tap do |error|
              raise Error, error.inspect if error.present?
            end
          end
        end
      end
    end

    def generate_destination_tag
      begin
        # Reserve destination 0 for system purpose
        tag = SecureRandom.random_number(10**10) + 1
      end while PaymentAddress.where(currency_id: :xrp)
                              .where('address LIKE ?', "%dt=#{tag}")
                              .any?
    end
  end
end
