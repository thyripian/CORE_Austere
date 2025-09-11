import React from 'react';
import '../styles/AboutComponent.css';

const AboutComponent = () => {
  return (
    <div className="about-content">

      <h2>
        <br></br>
        Part of the CORE enterprise.
      </h2>
      <p>
        The Centralized Operational Reporting Engine (CORE) was orignally developed <br />
        to facilitate databasing and querying of Word documents, Excel spreadsheets,<br />
        PowerPoint presentations, and Adobe PDFs that were stored on shared<br />
        drives, providing an efficient way to manage and search through large<br />
        collections of files without a practical solution in place. <br />

        <br></br>
        This lightweight version of the same application has been developed <br />
        specifically for use in offline and austere environments. It was built as a <br />
        BYOD (Bring Your Own Database) solution, allowing any prebuilt SQLite <br />
        database to be queried in real time, driving operational awareness and<br />
        fulfilling intelligence needs.<br />
      </p>
    </div>
  );
};

export default AboutComponent;
