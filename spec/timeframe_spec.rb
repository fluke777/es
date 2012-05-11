require 'es'

describe Es::Timeframe do

  it "should parse latest spec" do
    t = Es::Timeframe.parse("latest")
    binding.pry
    # t.from.should == 
  end


end