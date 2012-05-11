require 'es'

describe "helpers" do

  before :each do
    opts = {
      :file => '/a/b/user.csv',
      :fields => [
        Es::Field.new("User", 'recordid')
      ]
    }

    @entity = Es::Entity.new("Users", opts)
  end

  it "should transform the directory on webdav from filename and project PID" do
    Es::Helpers::load_destination_dir("1234", @entity).should == "1234"
  end

  it "should prvovide the web dav path" do
    Es::Helpers::web_dav_load_destination_dir('1234', @entity).should == "/uploads/1234"
  end

  it "should provide the web dav extract path" do
    Es::Helpers::web_dav_extract_destination_dir('1234', @entity).should == "/out_1234_Users"
  end

  it "should provide the web dav extract path" do
    Es::Helpers::extract_destination_dir('1234', @entity).should == "out_1234_Users"
  end

end