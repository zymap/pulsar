const React = require('react');
const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const siteConfig = require(`${process.cwd()}/siteConfig.js`);

class AdminRestApi extends React.Component {
  render() {
<<<<<<< HEAD
    const swaggerUrl = `${siteConfig.baseUrl}swagger/swagger.json`

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer" >
          <redoc spec-url={`${swaggerUrl}`} lazy-rendering="true"></redoc>
          <script src="//cdn.jsdelivr.net/npm/redoc/bundles/redoc.standalone.js"/>
=======

    const swaggerUrl = `${siteConfig.baseUrl}swagger/swagger.json`
    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer" >
          <script base-url={`${swaggerUrl}`} src="../js/getSwaggerByVersion.js"></script>
>>>>>>> f773c602c... Test pr 10 (#27)
        </Container>
      </div>
    );
  }
}

module.exports = AdminRestApi;
