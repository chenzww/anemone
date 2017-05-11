begin
  require 'mongo'
rescue LoadError
  puts "You need the mongo gem to use Anemone::Storage::MongoDB"
  exit
end

module Anemone
  module Storage
    class MongoDB

      BINARY_FIELDS = %w(body headers data)

      def initialize(mongo_db, collection_name)
        @client = mongo_db
        @db = @client.database
        @collection = @client[collection_name]
        @collection.create unless @db.collection_names.include?(@collection.name)
        @collection.delete_many
        @collection.indexes.create_one(url: 1)
      end

      def [](url)
        if value = @collection.find(url: url.to_s).first
          load_page(value)
        end
      end

      def []=(url, page)
        hash = page.to_hash
        BINARY_FIELDS.each do |field|
          hash[field] = BSON::Binary.new(hash[field]) unless hash[field].nil?
        end
        @collection.update_one({url: page.url.to_s}, hash, :upsert => true)
      end

      def delete(url)
        page = self[url]
        @collection.delete_many(url: url.to_s)
        page
      end

      def each
        @collection.find do |cursor|
          cursor.each do |doc|
            page = load_page(doc)
            yield page.url.to_s, page
          end
        end
      end

      def merge!(hash)
        hash.each { |key, value| self[key] = value }
        self
      end

      def size
        @collection.count
      end

      def keys
        keys = []
        self.each { |k, v| keys << k.to_s }
        keys
      end

      def has_key?(url)
        !!@collection.find(url: url.to_s).first
      end

      def close
        @client.close
      end

      private

      def load_page(hash)
        BINARY_FIELDS.each do |field|
          hash[field] = hash[field].to_s
        end
        Page.from_hash(hash)
      end

    end
  end
end

