require 'pry'
require 'es'

describe Es::Timeframe do
  
  before :each do
    @bare_spec = {
      :from => "today",
      :to   => "tomorrow"
    }
    @bare_timeframe = Es::Timeframe.parse(@bare_spec)
    
    @full_spec = {
      :from                 => "today",
      :to                   => "tomorrow",
      :interval             => 1,
      :interval_unit        => :month,
      :day_within_period    => :last
    }
    @timeframe = Es::Timeframe.parse(@full_spec)
  end

  it "should have to and from from spec" do
    @bare_timeframe.spec_from.should == "today"
    @bare_timeframe.spec_to.should == "tomorrow"
  end

  it "should have to and from" do
    @bare_timeframe.from.should == Chronic.parse('today')
    @bare_timeframe.to.should == Chronic.parse('tomorrow')
  end

  it "should have all the stuff" do
    @timeframe.interval.should == 1
    @timeframe.interval_unit.should == :month
    @timeframe.day_within_period.should == :last
  end

  it "should fail if interval is not a number" do
    @full_spec[:interval] = "s"
    lambda {timeframe = Es::Timeframe.parse(@full_spec)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if interval_unit is not an allowed value" do
    @full_spec[:interval_unit] = :xxx
    lambda {timeframe = Es::Timeframe.parse(@full_spec)}.should raise_error(Es::IncorrectSpecificationError)
  end

  it "should fail if day_within_period is not an allowed value" do
    @full_spec[:day_within_period] = :xxx
    lambda {timeframe = Es::Timeframe.parse(@full_spec)}.should raise_error(Es::IncorrectSpecificationError)
  end


  it "should fail if to or from is not provided" do
    spec = {
      
    }
    lambda {tf = Es::Timeframe.parse(spec)}.should raise_error(Es::InsufficientSpecificationError)
    
    spec = {
      :to => "today"
    }
    lambda {tf = Es::Timeframe.parse(spec)}.should raise_error(Es::InsufficientSpecificationError)
    
    spec = {
      :from => "today"
    }
    lambda {tf = Es::Timeframe.parse(spec)}.should raise_error(Es::InsufficientSpecificationError)
  end

  it "should fill in interval unit if not provided" do
    @bare_timeframe.interval_unit.should == :day
  end

  it "should fill in interval if not provided" do
    @bare_timeframe.interval.should == 1
  end

  it "should fill in day_within_period if not provided" do
    @bare_timeframe.day_within_period.should == :last
  end

end