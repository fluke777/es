require 'es'

describe "helpers" do
  it "should transform the directory on webdave from filename and project PID" do
    opts = {
      :file => '/a/b/user.csv',
      :fields => [
        Es::Field.new("User", 'recordid')
      ]
    }
    
    entity = Es::Entity.new("Users", opts)
    Es::Helpers::load_destination_dir("1234", entity).should == "/uploads/1234/Users"
    Es::Helpers::load_destination_file(entity, :with_date => true).should =~ /^user_....-..-.._..:..:..\.csv/
    Es::Helpers::load_destination_file(entity).should =~ /^user.csv/
  end
end