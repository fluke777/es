require 'es'
require 'pry'

describe Es::Extract do

  before :each do
    @load_spec = [
      {
        :entity => "User",
        :file   => "user_file",
        :fields => [
          {
            :name => "Id",
            :type => "recordid"
          }
        ]
      },
      {
        :entity => "User",
        :file   => "user_2_file",
        :fields => [
          {
            :name => "Name",
            :type => "fact"
          }
        ]
      },
      {
        :entity => "Opportunity",
        :file   => "op_file",
        :fields => [
          {
            :name => "Id",
            :type => "recordid"
          }
        ]
      }
    ]
    @load = Es::Load.parse(@load_spec)
    @extract_config = {
      :entities => [
        {
          :file   => "user.csv",
          :entity => "User",
          :fields => [
            "Id",
            "Name",
            "autoincrement",
            "snapshot"
          ]
        }
      ]
    }
    @extract = Es::Extract.parse(@extract_config, @load)
  end

  it "Should be able to instantiate programatically" do
    extract = Es::Extract.new([
      Es::Entity.new("User", {
        :fields => [
          Es::Field.new("Id", 'recordid')
          ],
        :file => 'something.csv'
      })
    ])
  end

  it "is able to parse from json and grab the info from loading columns" do
    @extract.entities.count.should == 1
    @extract.get_entity("User").fields.count.should == 4
    pp @extract.to_extract_fragment('123')
  end

  it "is able to parse timeframe" do
    t = Es::Extract.parse_timeframes({
      :from  => "1 weeks ago",
      :to    => "tomorrow"
    })
    t.should be_an_instance_of(Es::Timeframe)
  end
    
  it "is able to parse timeframes or array of timeframes" do
    t = Es::Extract.parse_timeframes(nil)
    t.should == nil
    
    t = Es::Extract.parse_timeframes([
      {
        :from  => "1 weeks ago",
        :to    => "tomorrow"
      },
      {
        :from  => "3 weeks ago",
        :to    => "1 weeks ago"
      }
    ])
    t.should be_an_instance_of(Array)
    t.count.should == 2
  end

  it "is able to parse latest timeframe" do
    t = Es::Extract.parse_timeframes("latest")
    t.should be_an_instance_of(Es::Timeframe)
  end

end

# describe Es::Extract do
# 
#   before :each do
#     @spec_with_timeframe = {
#       :name       => "User",
#       :fields     => ["Id", "Name", "Age"]
#     }
#     @extract_with_timeframe = Es::Extract.parse(@spec_with_timeframe)
#   end
# 
#   it "has name" do
#     @extract_with_timeframe.name.should == "User"
#   end
# 
#   it "has fields" do
#     @extract_with_timeframe.fields.should =~ ["Id", "Name", "Age"]
#   end
# 
#   # it "can be defined in code, not json" do
#   #   e = Es::Load.new([{
#   #     :name       => "User",
#   #     :file       => "my_file.json",
#   #     :fields     => [
#   #       Es::Field.new("Id", "recordid"),
#   #       Es::Field.new("Name", "attribute"),
#   #       Es::Field.new("Age", "fact")
#   #     ]
#   #   }])
#   # end
# end