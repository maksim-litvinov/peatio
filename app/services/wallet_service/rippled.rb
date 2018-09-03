# encoding: UTF-8
# frozen_string_literal: true

module WalletService
  class Rippled < Base
    def create_address(options = {})
      client.create_address!(options)
    end

    def collect_deposit!(deposit, options={})
      destination_address = destination_wallet(deposit).address
      pa = deposit.account.payment_address

      client.create_withdrawal!(
        { address: pa.address, secret: pa.secret },
        { address: destination_address },
        deposit.amount,
        options
      )
    end

    def build_withdrawal!(withdraw, options = {})
      client.create_withdrawal!(
        { address: wallet.address, secret: wallet.secret },
        { address: withdraw.rid },
        withdraw.amount,
        options
      )
    end
  end
end
