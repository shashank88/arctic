import React, { Component } from "react";
import "./App.css";

import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import AppBar from "@material-ui/core/AppBar";
import Toolbar from "@material-ui/core/Toolbar";
import Typography from "@material-ui/core/Typography";
import Button from "@material-ui/core/Button";

class App extends Component {
  constructor(props) {
    super(props);
    this.handleInitLibrary = this.handleInitLibrary.bind(this);

    this.state = {
      libraries: [],
      isLoading: false,
      error: null,
      display: false
    };
  }

  componentDidMount() {
    this.setState({ isLoading: true });

    fetch("/libraries/")
      .then(response => {
        console.log(response);
        if (response.ok) {
          return response.json();
        } else {
          throw new Error("Something went wrong ...");
        }
      })
      .then(data => this.setState({ libraries: data, isLoading: false }))
      .catch(error => this.setState({ error, isLoading: false }));
  }

  handleInitLibrary() {
    this.setState({ display: !this.state.display });
  }

  render() {
    const { libraries, isLoading, error } = this.state;

    if (error) {
      return <p>{error.message}</p>;
    }

    if (isLoading) {
      return <p>Loading ...</p>;
    }

    return (
      <div>
        <AppBar position="static" color="default">
          <Toolbar>
            <Typography variant="h6" color="inherit">
              Arctic-Explorer
            </Typography>
          </Toolbar>
        </AppBar>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Library </TableCell>
              <TableCell align="right">Type</TableCell>
              <TableCell align="right">Size</TableCell>
              <TableCell align="right">Quota</TableCell>
              <TableCell align="right">Versions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {libraries.map(lib => (
              <TableRow key={lib.name}>
                <TableCell component="th" scope="row">
                  {lib.name}
                </TableCell>
                <TableCell align="right">{lib.type}</TableCell>
                <TableCell align="right">{lib.used / 1024 / 1024} M</TableCell>
                <TableCell align="right">{lib.quota / 1024 / 1024} M</TableCell>
                <TableCell align="right">TODO</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <div>
          <Button variant="contained">Set quota</Button>
          <Button
            variant="contained"
            color="primary"
            onClick={this.handleInitLibrary}
          >
            Add library
          </Button>
          {this.state.display && <AddLibraryForm />}
          <Button variant="contained" color="secondary">
            Delete library
          </Button>
        </div>
      </div>
    );
  }
}


class AddLibraryForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {name: '', type: ''};

    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handleChange = ({target}) => {
    let key_name = target.name;
    this.setState({[key_name]: target.value});
  }

  handleSubmit(event) {
    fetch('/libraries/' + this.state.name, {
      method: 'POST',
      body: {
        'type': this.state.type
      }
    })
    event.preventDefault();
  }

  render() {
    return (
      <form onSubmit={this.handleSubmit}>
        <label>
          Name:
          <input name="name" type="text" value={this.state.name} onChange={this.handleChange} />
        </label>
        <label>
          Type:
          <input name="type" type="text" value={this.state.type} onChange={this.handleChange} />
        </label>
        <input type="submit" value="Submit" />
      </form>
    );
  }
}




export default App;
