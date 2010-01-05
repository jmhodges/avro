module Enumerable
  def hash_collect
    inject(Hash.new) do |memo, i|
      k, v = yield(i)
      memo[k] = v if k
      memo
    end
  end
end
