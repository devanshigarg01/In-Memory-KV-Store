class Parser {
    private : 
    vector<string> extractor(istream &input) {
        string word;
        getline(input, word);

        if (word.empty() || word[0] != '*') {
            throw runtime_error("Invalid RESP format");
        }

        int numArgs = stoi(word.substr(1));

        vector<string> data;
            
        for (int i =0; i <numArgs;++i)
            {
            data.push_back(parseBulkString(input));
        }
        return data;
    }
public:
    string parse(istream &input) {
        auto data = extractor(input);

        string command = data[0];
        transform(command.begin(), command.end(), command.begin(), ::tolower);

        switch(command) {
            case "PING" : break;
            case "PING" : break;
            case "PING" : break;
            case "PING" : break;
            case "PING" : break;
            case "PING" : break;
        }
    }

};