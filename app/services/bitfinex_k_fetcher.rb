# encoding: UTF-8
# frozen_string_literal: true

class BitfinexKFetcher
  CANDLES_API = "https://api.bitfinex.com/v2/candles/trade"
  MS = 1_000
  AVAILABLE_FRAMES = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D'].freeze

  attr_reader :redis

  def initialize(redis:)
    @redis = redis
  end

  def fetch_candle_data(market:, period:, start: nil)
    start = ENV.fetch('BITFINEX_START', Time.new(2018, 7, 1)) if start.nil?
    start = start.to_i
    frame = period_to_frame(period)
    return [] unless AVAILABLE_FRAMES.include?(frame)

    candles = fetch_bitfinex_data(market: market, start: start, frame: frame)
    push_to_redis(candles: candles, period: period, market: market)
    candles.first
  rescue StandardError => e
    report_exception(e)
  end

  private

  def fetch_bitfinex_data(market:, start:, frame:)
    Rails.logger.info { "Fetch data from bitfinex for market #{market} for frame #{frame} start from #{start}" }
    response = Faraday.get("#{CANDLES_API}:#{frame}:t#{market.upcase}/hist", start: start * MS)

    if response.status == 429
      Rails.logger.info { "Rate limit exceeded. Sleep a minute" }
      sleep(60)
      return []
    end

    response.assert_success!
            .yield_self { |r| JSON.parse(r.body) || [] }
            .yield_self do |candles|
              candles.map do |candle|
                mts, open, close, high, low, volume = candle
                [mts / MS, open, close, high, low, volume.round(4)]
              end.sort
            end
  end

  def period_to_frame(period)
    return "#{period}m" if period < 60
    return "#{period / 60}h"  if period < 1440

    "#{period / 60 / 24}D"
  end

  def push_to_redis(candles:, market:, period:)
    return if candles.blank?
    redis.rpush(key(market, period), candles)
  end

  def key(market, period)
    "peatio:#{market}:k:#{period}"
  end
end
